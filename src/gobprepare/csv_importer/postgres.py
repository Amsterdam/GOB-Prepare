import time

from pandas import read_csv
from pandas.errors import ParserError
from urllib.error import HTTPError

from gobcore.database.writer.postgresql import execute_postgresql_query, write_rows_to_postgresql
from gobcore.exceptions import GOBException


class PostgresCsvImporter():
    """
    Imports a CSV file into a Postgres table
    """
    MAX_RETRIES = 5
    WAIT_RETRY = 2

    def __init__(self, dst_connection, config: dict):
        self._dst_connection = dst_connection
        self._source = config['source']
        self._destination = config['destination']

    def _load_csv(self):
        """Loads CSV with data. Downloads over HTTP if necessary. Returns the datum and column metadata for further
        processing.

        :param columns:
        :return:
        """
        tries = 0
        while True:
            try:
                df = read_csv(self._source)
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
                "name": col,
            } for col in df.columns],

            # List of lists of values
            "data": [list(map(lambda x: str(x), row.tolist())) for _, row in df.iterrows()]
        }

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
