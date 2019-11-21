import time
import tempfile
import os

from pandas import read_csv
from pandas.errors import ParserError
from urllib.error import HTTPError

from gobcore.database.connector import connect_to_objectstore
from gobcore.database.writer.postgresql import execute_postgresql_query, write_rows_to_postgresql
from gobcore.exceptions import GOBException

from gobprepare.config import get_objectstore_config, CONTAINER_BASE


class PostgresCsvImporter():
    """
    Imports a CSV file into a Postgres table
    """
    MAX_RETRIES = 5
    WAIT_RETRY = 2

    def __init__(self, dst_connection, config: dict):
        self._dst_connection = dst_connection
        self._destination = config['destination']

        if config.get('objectstore'):
            self._source = self._load_from_objectstore(config['objectstore'], config['source'])
        else:
            self._source = config['source']

        # Mapping of CSV columns to database columns (default CSV columns will be used if no alternative supplied)
        self._column_names = config.get('column_names', {})
        self._separator = config.get('separator', ',')

    def _is_empty_row(self, row):
        """Returns True if pandas row is empty

        :param row:
        :return:
        """
        return all([True if i is None or not str(i) else False for i in row.tolist()])

    def _load_csv(self):
        """Loads CSV with data. Downloads over HTTP if necessary. Returns the datum and column metadata for further
        processing.

        :param columns:
        :return:
        """
        tries = 0
        while True:
            try:
                df = read_csv(self._source, keep_default_na=False, sep=self._separator)
                break
            except ParserError:
                raise GOBException(f"Can't parse CSV: {self._source}")
            except HTTPError:
                tries += 1
                if tries >= self.MAX_RETRIES:
                    raise GOBException(f"Problems downloading CSV: {self._source}")

                time.sleep(self.WAIT_RETRY)

        return {
            "columns": [{
                "max_length": max([len(str(i)) for i in df[col]]),
                "name": self._column_names.get(col, col),
            } for col in df.columns],

            # List of lists of values
            "data": [list(map(lambda x: str(x) if x else None, row.tolist())) for _, row in df.iterrows()
                     if not self._is_empty_row(row)]
        }

    def _tmp_filename(self, filename: str):
        new_location = os.path.join(tempfile.gettempdir(), filename)
        os.makedirs(os.path.dirname(new_location), exist_ok=True)
        return new_location

    def _load_from_objectstore(self, objectstore: str, location: str):
        new_location = self._tmp_filename(location)

        connection, user = connect_to_objectstore(get_objectstore_config(objectstore))
        obj = connection.get_object(CONTAINER_BASE, location)[1]
        with open(new_location, 'wb') as fp:
            fp.write(obj)
        return new_location

    def _create_destination_table(self, columns: list):
        """Creates destination table

        :param columns:
        :return:
        """
        columndefs = ",".join([f"{col['name']} VARCHAR({col['max_length'] + 5}) NULL" for col in columns])
        query = f"CREATE TABLE {self._destination} ({columndefs})"
        execute_postgresql_query(self._dst_connection, query)

    def _import_data(self, data: list):
        """Writes CSV data to destination table

        :param data:
        :return:
        """
        write_rows_to_postgresql(self._dst_connection, self._destination, data)

    def import_csv(self):
        """Entry method. Returns number of inserted rows

        :return:
        """
        data = self._load_csv()
        self._create_destination_table(data['columns'])
        self._import_data(data['data'])
        return len(data['data'])
