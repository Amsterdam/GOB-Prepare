import http.client
from functools import partial
from io import StringIO
from unittest import TestCase
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import pandas as pd
from pandas import read_csv
from pandas.errors import ParserError
from pandas.io.parsers import TextFileReader
from swiftclient import Connection

from gobcore.datastore.sql import SqlDatastore
from gobcore.exceptions import GOBException
from gobprepare.importers.dump_importer import SqlDumpImporter, ObjectDatastore, _load_from_objectstore
from gobprepare.importers.typing import SqlDumpImporterConfig
from gobprepare.utils.requests import retry


def patch_retry(max_retry, wait, func, self):
    return retry(max_retry, wait)(partial(func.__wrapped__, self))


class TestSqlCsvImporter(TestCase):
    """Test SqlCsvImporter."""

    def setUp(self) -> None:
        self.config_objectstore: SqlDumpImporterConfig = {
            "id": "cfg_objectstore",
            "depends_on": ["other_cfg"],
            "type": "import_dump",
            "read_config": {
                "file_filter": "http://example.com/somefile.sql.gz",
                "container": "some_container",
                "filter_list": "STRING1|STRING2|STRING3|string4",
                "substitution": {
                    "pattern_1": "replace_1",
                    "pattern_2": "replace_2"
                },
                "comments_regexp": "some_comments_regexp",
                "split_regexp": "some_split_regexp",
                "data_delimiter_regexp": "some_data_delimiter_regexp",
                "copy_query_regex": "some_copy_query_regex"
            },
            "destination": "schema.table",
            "objectstore": "TheObjectstore"
        }

        self.config_source: SqlDumpImporterConfig = {
            "id": "cfg_source",
            "depends_on": ["other_cfg"],
            "type": "import_dump",
            "destination": "schema.table",
            "source": "the source",
        }

        self.dst_datastore = MagicMock(spec_set=SqlDatastore)
        self.os_importer = SqlDumpImporter(self.dst_datastore, self.config_objectstore)
        self.url_importer = SqlDumpImporter(self.dst_datastore, self.config_source)

    def test_init(self):
        assert self.os_importer._dst_datastore == self.dst_datastore
        assert self.os_importer._destination == self.config_objectstore["destination"]
        assert self.os_importer._read_config == self.config_objectstore["read_config"]
        assert self.os_importer._objectstore == self.config_objectstore["objectstore"]
        assert self.os_importer._source is None
        assert self.os_importer._encoding == "utf-8"
        
        assert self.url_importer._dst_datastore == self.dst_datastore
        assert self.url_importer._destination == self.config_source["destination"]
        assert self.url_importer._read_config is None
        assert self.url_importer._objectstore is None
        assert self.url_importer._encoding == "utf-8"
        assert self.url_importer._source == "the source"

    @patch("gobprepare.importers.csv_importer.tempfile")
    def test_load_from_objectstore(self, mock_tmpfile):
        mock_datastore = MagicMock(spec=ObjectDatastore)
        mock_datastore.container_name = "my container base"
        mock_datastore.query.return_value = iter([{"name": "file/location/on/objectstore.ext"}])

        mock_datastore.connection = MagicMock(spec=Connection)
        mock_datastore.connection.get_object.return_value = {}, ["the object data"]

        mock_tmpfile.NamedTemporaryFile.return_value.__enter__.return_value.name = "my src file"

        with _load_from_objectstore(mock_datastore) as src_file:
            assert src_file == "my src file"

        mock_tmpfile.NamedTemporaryFile.assert_called_with(mode="wb", suffix=".csv")
        mock_tmpfile.NamedTemporaryFile.return_value.__enter__.return_value.write.assert_called_with("the object data")

        mock_datastore.connect.assert_called()
        mock_datastore.disconnect.assert_called()
        mock_datastore.connection.get_object.assert_called_with(
            container="my container base",
            obj="file/location/on/objectstore.ext",
            resp_chunk_size=100_000_000
        )
