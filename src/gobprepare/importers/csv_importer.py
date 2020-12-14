import time
import tempfile
import os

from pandas import read_csv
from pandas.errors import ParserError
from urllib.error import HTTPError

from gobcore.exceptions import GOBException

from gobprepare.config import CONTAINER_BASE
from gobcore.datastore.sql import SqlDatastore
from gobcore.datastore.factory import DatastoreFactory
from gobcore.datastore.objectstore import ObjectDatastore
from gobconfig.datastore.config import get_datastore_config


class SqlCsvImporter():
    """
    Imports a CSV file into a SqlDatastore table
    """
    MAX_RETRIES = 5
    WAIT_RETRY = 2

    def __init__(self, dst_datastore: SqlDatastore, config: dict):
        self._dst_datastore = dst_datastore
        self._destination = config['destination']

        if config.get('objectstore'):
            self._source = self._load_from_objectstore(config['objectstore'], config['read_config'])
        elif config.get('source'):
            self._source = config.get('source')
        else:
            raise GOBException("Incomplete config. Expecting key 'objectstore' or 'source'")

        # Mapping of CSV columns to database columns (default CSV columns will be used if no alternative supplied)
        self._column_names = config.get('column_names', {})
        self._separator = config.get('separator', ',')
        self._encoding = config.get('encoding', 'utf-8')

    def _is_empty_row(self, row):
        """Returns True if pandas row is empty

        :param row:
        :return:
        """
        return all([True if i is None or not str(i) else False for i in row.tolist()])

    def _load_csv(self):
        """Loads CSV with data. Downloads over HTTP if necessary. Returns the datum and column metadata for further
        processing.

        :return:
        """
        tries = 0
        while True:
            try:
                df = read_csv(
                    self._source,
                    keep_default_na=False,
                    sep=self._separator,
                    dtype=str,
                    encoding=self._encoding
                )
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

    def _load_from_objectstore(self, objectstore: str, read_config: dict):
        objectstore = DatastoreFactory.get_datastore(get_datastore_config(objectstore), read_config)
        assert isinstance(objectstore, ObjectDatastore), "Expected Objectstore"

        objectstore.connect()

        try:
            obj_info = next(objectstore.query(None))
        except StopIteration:
            raise GOBException(f"File not found on Objectstore: {read_config['file_filter']}")

        new_location = self._tmp_filename(obj_info['name'])
        obj = objectstore.connection.get_object(CONTAINER_BASE, obj_info['name'])[1]

        with open(new_location, 'wb') as fp:
            fp.write(obj)
        return new_location

    def _create_destination_table(self, columns: list):
        """Creates destination table

        :param columns:
        :return:
        """

        # This works for Postgres and probably for most SQL databases
        columndefs = ",".join([f"{col['name']} VARCHAR({col['max_length'] + 5}) NULL" for col in columns])
        query = f"CREATE TABLE {self._destination} ({columndefs})"
        self._dst_datastore.execute(query)

    def _import_data(self, data: list):
        """Writes CSV data to destination table

        :param data:
        :return:
        """
        self._dst_datastore.write_rows(self._destination, data)

    def import_csv(self):
        """Entry method. Returns number of inserted rows

        :return:
        """
        data = self._load_csv()
        self._create_destination_table(data['columns'])
        self._import_data(data['data'])
        return len(data['data'])
