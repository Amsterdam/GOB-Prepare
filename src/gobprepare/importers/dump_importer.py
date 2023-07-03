import contextlib
from io import StringIO
import tempfile
import gzip
import re
import tempfile
from typing import List, Iterator, Optional

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

    obj_info = next(datastore.query(None), None)

    if obj_info is None:
        raise GOBException(f"File not found on Objectstore: {datastore.read_config['file_filter']}")

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
        """ Initialise SqlDumpImporter """
        self._dst_datastore = dst_datastore
        # self._destination = config["destination"]
        self._encoding = config.get("encoding", "utf-8")

        self._read_config: Optional[ReadConfig] = config.get("read_config")
        self._objectstore: Optional[str] = config.get("objectstore")
        self._source: Optional[str] = config.get("source")

    def _process_queries(self, queries: list) -> None:
        for query in queries:
            # remove leading new line '\n\n'
            query = query.lstrip()
            # filter out and replace the queries based on the filter and substitution lists
            if (query and not re.match(self._read_config['filter_list'], query)):
                if not query.endswith(self._read_config['data_delimiter_regexp']):
                    query += ';'
                    for pattern, replacement in self._read_config['substitution'].items():
                        query = re.sub(pattern, replacement, query)

                if re.match(self._read_config['copy_query_regex'], query):
                    copy_query = query
                    continue

                if re.search(self._read_config['data_delimiter_regexp'], query):
                    data = query.split(self._read_config['data_delimiter_regexp'])
                    if len(data[0]) != 0:
                        self._dst_datastore.copy_expert(copy_query, StringIO(data[0]))
                    continue

                self._dst_datastore.execute(query)


    def _import_dump(self, source_path: str) -> str:
        decompressed_file = gzip.GzipFile(source_path, 'r')
        file_content = decompressed_file.read().decode('utf-8')
        logger.info(f'Processing decompressed content of file "{self._read_config["file_filter"].split("/")[1]}"')

        # remove all comments
        sql_queries = [line for line in re.split(self._read_config['comments_regexp'], file_content) if not line == None and not line.isspace() and line.strip()]
        # filter out the queries
        for query_list in sql_queries:
            # split on ';' for the sql queries en on '\.' for the data
            queries = re.split(self._read_config['split_regexp'], query_list)
            self._process_queries(queries)
        return self._read_config["file_filter"].split('/')[1]

    def import_dumps(self) -> int:
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
