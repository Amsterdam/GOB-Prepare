import contextlib
from io import StringIO
import tempfile
import re
from typing import Iterator, Optional

from gzip import GzipFile
from gobprepare.importers.typing import ReadConfig, SqlDumpImporterConfig
from gobconfig.datastore.config import get_datastore_config
from gobcore.datastore.factory import DatastoreFactory
from gobcore.datastore.objectstore import ObjectDatastore
from gobcore.datastore.sql import SqlDatastore
from gobcore.datastore.postgres import PostgresDatastore

from gobcore.logging.logger import logger

from gobcore.exceptions import GOBException


@contextlib.contextmanager
def _load_from_objectstore(datastore: ObjectDatastore) -> Iterator[str]:
    datastore.connect()
    try:
        obj_info = next(datastore.query(None))
    except StopIteration:
        raise GOBException(f"File '{datastore.read_config['file_filter']}' not found on Objectstore.")
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
    """Import a SQL dump into a PostgresDatastore table."""

    def __init__(self, dst_datastore: PostgresDatastore, config: SqlDumpImporterConfig) -> None:
        """Initialise SqlDumpImporter"""
        self._dst_datastore = dst_datastore
        self._encoding = config.get("encoding", "utf-8")
        self._read_config: ReadConfig = config["read_config"]
        self._objectstore: Optional[str] = config.get("objectstore")
        self._source: Optional[str] = config.get("source")

    def _perform_query_substitutions(self, query: str) -> str:
        # perform the substitutions as listed in read_config.
        if not query.endswith(self._read_config["data_delimiter_regexp"]):
            query += ";"
            for pattern, replacement in self._read_config["substitution"].items():
                query = re.sub(pattern, replacement, query)
        return query    

    def _instert_data(self, copy_query: str, data: str) -> None:
        # ignore the data delimiter '\.'
        data_to_insert = data.split(self._read_config["data_delimiter_regexp"])
        if len(data[0]) != 0:
            self._dst_datastore.copy_from_stdin(copy_query, StringIO(data_to_insert[0]))

    def _process_and_execute_queries(self, queries: list[str]) -> None:
        """Filter out, replace paramaters using lists in read_config.
        then execute the queries

        :return: None
        """
        filter_list_pattern = re.compile(self._read_config["filter_list"])
        copy_query_pattern = re.compile(self._read_config["copy_query_regex"])
        data_delimiter_pattern = re.compile(self._read_config["data_delimiter_regexp"])

        for query in queries:
            # remove leading new line '\n\n'
            query = query.lstrip()
            # Filter out the queries using filter_list defined in read_config and then execute the queries.
            if query and not re.match(filter_list_pattern, query):
                query = self._perform_query_substitutions(query)
                # the copy (insert) query is in the form:
                #       COPY <table name> (colomn1, colomn 2, ...) FROM stdin;
                # the copy query is always followed by the data in the form:
                #       "100000000008718135	Vennoot;	100000000008718134	100000000008718136	OnbeperktBevoegd	0
                #       100000000008718135	Vennoot	100000000008718134	100000000008718136	OnbeperktBevoegd	0
                #       \."
                # extract the copy query and execute it in the next iteration on the to insert data.
                if re.match(copy_query_pattern, query):
                    copy_query = query
                    continue

                # extract and insert data using copy_query.
                if re.search(data_delimiter_pattern, query):
                    data = query
                    self._instert_data(copy_query, data)
                    continue

                # execute other (regular) queries
                self._dst_datastore.execute(query)

    def _extract_sql_queries(self, content):
        # remove all comments and other non-executable lines (e.g empty lines and None resulting from split() action).
        lines = re.split(self._read_config["comments_regexp"], content)
        for line in lines:
            if line is not None and line.strip():
                yield line

    def _import_dump(self, source_path: str) -> str:
        decompressed_file = GzipFile(source_path, "r")
        file_content = decompressed_file.read().decode("utf-8")
        logger.info(f'Processing decompressed content of dump "{self._read_config["file_filter"].split("/")[-1]}"')

        # Extract the SQL queries from the content.
        sql_queries = self._extract_sql_queries(file_content)
        split_pattern = re.compile(self._read_config["split_regexp"])

        for query_list in sql_queries:
            # split on ';' for the sql queries en on '\.' for the data
            queries = re.split(split_pattern, query_list)
            self._process_and_execute_queries(queries)

        return self._read_config["file_filter"].split("/")[-1]

    def import_dump(self) -> str:
        """Entry method. Return (processed) gz file name.

        :return: str
        """
        if self._objectstore and not self._source:
            datastore = DatastoreFactory.get_datastore(get_datastore_config(self._objectstore), self._read_config)

            if not isinstance(datastore, ObjectDatastore):
                raise GOBException(f"Expected objectstore, got: {type(datastore)}")
            with _load_from_objectstore(datastore) as src_dump_file:
                return self._import_dump(src_dump_file)
        elif not self._objectstore and self._source:
            return self._import_dump(self._source)
        else:
            raise GOBException("Incomplete config. Expecting key 'objectstore' or 'source'")
