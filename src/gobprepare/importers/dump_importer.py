import contextlib
import tempfile
import datetime
import gzip
import logging
import os, shutil
import re
import tempfile

from dateutil import parser
from typing import List, Iterator, Optional

from gobprepare.importers.typing import ReadConfig, SqlDumpImporterConfig
from gobconfig.datastore.config import get_datastore_config
from gobcore.datastore.factory import DatastoreFactory
from gobcore.datastore.objectstore import ObjectDatastore
from gobcore.datastore.sql import SqlDatastore
from gobcore.logging.logger import logger

from gobcore.exceptions import GOBException

filter_kvk_list = ['GRANT', 'DROP', 'OWNER', 'search_path', 'REVOKE', 'CREATE TRIGGER', 'CREATE INDEX', 'ADD CONSTRAINT', 'ALTER TABLE', 'PRIMARY KEY (']

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
            print(f"File loaded from objectstore: {obj_info['name']}")
            yield fp.name
        finally:
            datastore.disconnect()


class SqlDumpImporter:
    """Import a SQL dump into a SqlDatastore table."""
    # TODO: 
    def __init__(self, dst_datastore: SqlDatastore, config: SqlDumpImporterConfig) -> None:
        """ Initialise SqlDumpImporter """
        self._dst_datastore = dst_datastore
        self._destination = config["destination"]
        self._encoding = config.get("encoding", "utf-8")

        self._read_config: Optional[ReadConfig] = config.get("read_config")
        self._objectstore: Optional[str] = config.get("objectstore")
        self._source: Optional[str] = config.get("source")

    def _import_dump(self, source_path: str) -> None:
        decompressed_file = gzip.GzipFile(source_path, 'r')
        file_content = decompressed_file.read().decode('utf-8')
        # remove empty and comment lines
        sql_queries = [line for line in file_content.split('\n') if line.strip() and not line.startswith('--')]
        # filter out the queries
        for query in sql_queries:
            if (not any(word in query for word in filter_kvk_list)):
                query = re.sub(r'^.*geometry\(Point.*$', ' geopunt GEOMETRY(Point,28992)', query)
                query = re.sub(r'igp_[a-zA-Z0-9_]+_cmg_owner\.', '', query)
                # Execute query
                self._dst_datastore.execute(query)

    def import_dumps(self) -> int:
        """Entry method. Return number of impoted files.

        :return:
        """
        if self._objectstore and not self._source:
            datastore = DatastoreFactory.get_datastore(get_datastore_config(self._objectstore), self._read_config)

            if not isinstance(datastore, ObjectDatastore):
                raise GOBException(f"Expected objectstore, got: {type(datastore)}")

            with _load_from_objectstore(datastore) as src_file:
                return self._import_dump(src_file)
        elif not self._objectstore and self._source:
            return self._import_dump(self._source)
        else:
            raise GOBException("Incomplete config. Expecting key 'objectstore' or 'source'")
