import contextlib
import http.client
import tempfile
import time
from typing import Optional
from urllib.error import HTTPError

from gobconfig.datastore.config import get_datastore_config
from gobcore.datastore.factory import DatastoreFactory
from gobcore.datastore.objectstore import ObjectDatastore
from gobcore.datastore.sql import SqlDatastore
from gobcore.exceptions import GOBException
from pandas import read_csv, NA as pd_NA
from pandas.io.parsers import TextFileReader
from pandas.errors import ParserError

from gobprepare.importers.typing import ReadConfig, SqlCsvImporterConfig
from gobprepare.utils.postgres import create_table_columnar_query


class SqlCsvImporter:
    """Import a CSV file into a SqlDatastore table."""

    MAX_RETRIES = 5
    WAIT_RETRY = 2

    CSV_CHUNK_SIZE = 100_000

    def __init__(self, dst_datastore: SqlDatastore, config: SqlCsvImporterConfig) -> None:
        """Initialise SqlCsvImporter."""
        self._dst_datastore = dst_datastore
        self._destination = config["destination"]

        self._read_config: ReadConfig = config.get("read_config")
        self._objectstore: str = config.get("objectstore")
        self._source: str = config.get("source")

        if not bool(self._objectstore) ^ bool(self._source):
            raise GOBException("Incomplete config. Expecting key 'objectstore' or 'source'")

        # Mapping of CSV columns to database columns (default CSV columns will be used if no alternative supplied)
        self._column_names = config.get("column_names", {})
        self._separator = config.get("separator", ",")
        self._encoding = config.get("encoding", "utf-8")

    def _load_csv(self, reader: TextFileReader) -> tuple[list[str], list[tuple[Optional[str]]]]:
        """Load CSV with data.

        Download over HTTP if necessary.
        Return the datum and column metadata for further processing.

        :return:
        """
        tries = 0

        while True:
            try:
                df = reader.get_chunk(self.CSV_CHUNK_SIZE)
            except StopIteration:
                return [], []
            except ParserError:
                raise GOBException(f"Can't parse CSV: {self._source}")
            except (HTTPError, http.client.HTTPException):
                tries += 1
                if tries >= self.MAX_RETRIES:
                    raise GOBException(f"Problems downloading CSV: {self._source}")

                time.sleep(self.WAIT_RETRY)
            else:
                # Drop empty rows
                df.dropna(axis="index", how="all", inplace=True)
                df.replace(to_replace=pd_NA, value=None, inplace=True)

                columns = [self._column_names.get(col, col) for col in df]
                data = [row for row in zip(*[df[col] for col in df])]

                return columns, data

    @contextlib.contextmanager
    def _load_from_objectstore(self, datastore: ObjectDatastore):
        datastore.connect()

        obj_info = next(datastore.query(None), None)

        if obj_info is None:
            raise GOBException(f"File not found on Objectstore: {self._read_config['file_filter']}")

        _, obj = datastore.connection.get_object(
            container=datastore.container_name,
            obj=obj_info["name"],
            resp_chunk_size=100_000_000
        )

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv") as fp:
            try:
                for chunk in obj:
                    fp.write(chunk)
                print(f"File loaded from objectstore: {obj_info['name']}")
                yield fp.name
            finally:
                datastore.disconnect()

    def _create_destination_table(self, columns: list[str]) -> None:
        """Create a destination table.

        :param columns:
        :return:
        """
        # This works for Postgres and probably for most SQL databases
        columndefs = ",".join([f"\"{col}\" TEXT NULL" for col in columns])
        query = create_table_columnar_query(self._dst_datastore, self._destination, columndefs)
        self._dst_datastore.execute(query)

    def _import_data(self, data: list[tuple[Optional[str]]]) -> int:
        """Write CSV data to destination table.

        :param data:
        :return:
        """
        return self._dst_datastore.write_rows(self._destination, data)

    def _import_csv(self, source_path: str) -> int:
        inserted_rows = 0

        with read_csv(
            source_path,
            keep_default_na=False,
            na_values="",  # only convert empty strings to NaN
            sep=self._separator,
            dtype=str,  # force string dtypes
            encoding=self._encoding,
            iterator=True
        ) as reader:

            while True:
                columns, data = self._load_csv(reader)

                if not data:
                    break
                if not inserted_rows:
                    self._create_destination_table(columns)

                inserted_rows += self._import_data(data)

        if inserted_rows > 0:
            self._dst_datastore.execute(f"ANALYZE {self._destination}")

        return inserted_rows

    def import_csv(self) -> int:
        """Entry method. Return number of inserted rows.

        :return:
        """
        if self._objectstore:
            datastore = DatastoreFactory.get_datastore(get_datastore_config(self._objectstore), self._read_config)
            with self._load_from_objectstore(datastore) as src_file:
                return self._import_csv(src_file)
        else:
            return self._import_csv(self._source)
