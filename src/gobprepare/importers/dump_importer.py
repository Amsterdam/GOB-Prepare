import contextlib
from io import StringIO
import tempfile
import gzip
import re
from typing import Iterator, Optional

from gobprepare.importers.typing import ReadConfig, SqlDumpImporterConfig
from gobconfig.datastore.config import get_datastore_config
from gobcore.datastore.factory import DatastoreFactory
from gobcore.datastore.objectstore import ObjectDatastore
from gobcore.datastore.sql import SqlDatastore
from gobcore.logging.logger import logger

from gobcore.exceptions import GOBException


@contextlib.contextmanager
def _load_from_objectstore(datastore: ObjectDatastore) -> Iterator[str]:
    datastore.connect()
    try:
        obj_info = next(datastore.query(None), None)
    except StopIteration:
        GOBException(f"File not found on Objectstore: {datastore.read_config['file_filter']}")
    else:
        _, obj = datastore.connection.get_object(
            container=datastore.container_name, obj=obj_info["name"], resp_chunk_size=100_000_000
        )

    with tempfile.NamedTemporaryFile(mode="wb", suffix=".gz") as fp:
        try:
            for chunk in obj:
                fp.write(chunk)
            logger.info(f"File loaded from objectstore: {obj_info['name']}")
            yield fp.name
        finally:
            datastore.disconnect()


class SqlDumpImporter:
    """Import a SQL dump into a SqlDatastore table."""

    def __init__(self, dst_datastore: SqlDatastore, config: SqlDumpImporterConfig) -> None:
        """Initialise SqlDumpImporter"""
        self._dst_datastore = dst_datastore
        self._encoding = config.get("encoding", "utf-8")

        self._read_config: ReadConfig = config["read_config"]
        self._objectstore: Optional[str] = config.get("objectstore")
        self._source: Optional[str] = config.get("source")

    def _substitute_query(self, query: str) -> str:
        if not query.endswith(self._read_config["data_delimiter_regexp"]):
            query += ";"
            for pattern, replacement in self._read_config["substitution"].items():
                query = re.sub(pattern, replacement, query)
        return query

    def _instert_data(self, copy_query: str, data: str) -> None:
        # ingnore the data delimiter '\.'
        data = data.split(self._read_config["data_delimiter_regexp"])
        if len(data[0]) != 0:
            self._dst_datastore.copy_expert(copy_query, StringIO(data[0]))

    def _process_queries(self, queries: list[str]) -> None:
        """Filter out, replace some paramaters and execute the queries
        :return: None
        """
        filter_list_pattern = re.compile(self._read_config["filter_list"])
        copy_query_pattern = re.compile(self._read_config["copy_query_regex"])
        data_delimiter_pattern = re.compile(self._read_config["data_delimiter_regexp"])

        for query in queries:
            # remove leading new line '\n\n'
            query = query.lstrip()
            # Filter out the queries based on words defined in filter_list and then execute the (eventuele manipulated) queries.
            # e.g. sql statements like "ALTER TABLE <table> OWNER TO <owner>;", " CREATE TRIGGER..", or "ADD CONSTRAINT ...PRIMARY KEY ..."
            if query and not re.match(filter_list_pattern, query):
                # Replace (if applicable) some params in some SQL queries (not in to insert data).
                # E.g. change database name to gob schema name "hr", and geometry type (corresponding Postgres geometry type)
                query = self._substitute_query(query)

                # get copy from stdin query
                if re.match(copy_query_pattern, query):
                    copy_query = query
                    continue
                # get and insert data
                if re.search(data_delimiter_pattern, query):
                    self._instert_data(copy_query, query)
                    continue

                self._dst_datastore.execute(query)

    def _extract_sql_queries(self, content):
        # remove all comments and other non-executable lines (e.g empty lines and None resulting from split() action).
        lines = re.split(self._read_config["comments_regexp"], content)
        for line in lines:
            if line is not None and line.strip():
                yield line

    def _import_dump(self, source_path: str) -> str:
        decompressed_file = gzip.GzipFile(source_path, "r")
        file_content = decompressed_file.read().decode("utf-8")
        logger.info(f'Processing decompressed content of dump "{self._read_config["file_filter"].split("/")[-1]}"')

        # Extract the SQL queries or statements from the content.
        sql_queries = list(self._extract_sql_queries(file_content))

        split_pattern = re.compile(self._read_config["split_regexp"])
        # preocess and execute SQL queries
        for query_list in sql_queries:
            # split on ';' for the sql queries en on '\.' for the data
            queries = re.split(split_pattern, query_list)
            self._process_queries(queries)

        return self._read_config["file_filter"].split("/")[-1]

    def import_dumps(self) -> str:
        """Entry method. Return (processed) gz file name.

        :return: str
        """
        if self._objectstore and not self._source:
            datastore = DatastoreFactory.get_datastore(
                get_datastore_config(self._objectstore),
                self._read_config
            )

            if not isinstance(datastore, ObjectDatastore):
                raise GOBException(f"Expected objectstore, got: {type(datastore)}")
            with _load_from_objectstore(datastore) as src_dump_file:
                return self._import_dump(src_dump_file)
        elif not self._objectstore and self._source:
            return self._import_dump(self._source)
        else:
            raise GOBException("Incomplete config. Expecting key 'objectstore' or 'source'")
