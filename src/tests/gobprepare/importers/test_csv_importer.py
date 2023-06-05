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
from gobprepare.importers.csv_importer import SqlCsvImporter, ObjectDatastore, _load_from_objectstore
from gobprepare.importers.typing import SqlCsvImporterConfig
from gobprepare.utils.requests import retry


def patch_retry(max_retry, wait, func, self):
    return retry(max_retry, wait)(partial(func.__wrapped__, self))


class TestSqlCsvImporter(TestCase):
    """Test SqlCsvImporter."""

    def setUp(self) -> None:
        self.config_objectstore: SqlCsvImporterConfig = {
            "id": "cfg_objectstore",
            "depends_on": ["other_cfg"],
            "type": "import_csv",
            "read_config": {
                "file_filter": "http://example.com/somefile.csv",
            },
            "destination": "schema.table",
            "objectstore": "TheObjectstore"
        }

        self.config_source: SqlCsvImporterConfig = {
            "id": "cfg_source",
            "depends_on": ["other_cfg"],
            "type": "import_csv",
            "destination": "schema.table",
            "source": "the source",
        }

        self.dst_datastore = MagicMock(spec_set=SqlDatastore)
        self.os_importer = SqlCsvImporter(self.dst_datastore, self.config_objectstore)
        self.os_importer._load_csv_chunk = patch_retry(2, 0, SqlCsvImporter._load_csv_chunk, self.os_importer)
        self.url_importer = SqlCsvImporter(self.dst_datastore, self.config_source)

    def test_init(self):
        assert self.os_importer._dst_datastore == self.dst_datastore
        assert self.os_importer._destination == self.config_objectstore["destination"]
        assert self.os_importer._read_config == self.config_objectstore["read_config"]
        assert self.os_importer._objectstore == self.config_objectstore["objectstore"]
        assert self.os_importer._source is None
        assert self.os_importer._column_names == {}
        assert self.os_importer._separator == ","
        assert self.os_importer._encoding == "utf-8"
        
        assert self.url_importer._dst_datastore == self.dst_datastore
        assert self.url_importer._destination == self.config_source["destination"]
        assert self.url_importer._read_config is None
        assert self.url_importer._objectstore is None
        assert self.url_importer._column_names == {}
        assert self.url_importer._separator == ","
        assert self.url_importer._encoding == "utf-8"
        assert self.url_importer._source == "the source"

    def test_init_non_defaults(self):
        self.config_objectstore["column_names"] = {"csv_column": "db_column"}
        self.config_objectstore["separator"] = ";"
        self.config_objectstore["encoding"] = "iso-8859-1"

        importer = SqlCsvImporter(self.dst_datastore, self.config_objectstore)

        assert importer._separator == ";"
        assert importer._column_names == {"csv_column": "db_column"}
        assert importer._encoding == "iso-8859-1"

    def test_read_csv(self):
        df = read_csv(
            StringIO("col1;col2;col3\nrow1col1;row1 col 2;nan\nro w2c o l1;row2 col 2;\n;;\n"),
            keep_default_na=False,
            na_values="",  # only convert empty strings to NaN
            sep=";",
            dtype=str,  # force string dtypes
            encoding="utf-8",
            index_col=False
        )
        # expected is transposed
        expected = [
            ["row1col1", "ro w2c o l1", "<NA>"],
            ["row1 col 2", "row2 col 2", "<NA>"],
            ["nan", "<NA>", "<NA>"]
        ]
        assert expected == [[val if pd.notna(val) else "<NA>" for val in df[col]] for col in df]

    def test_load_csv_chunk(self):
        mock_reader = MagicMock(spec_set=TextFileReader)
        self.os_importer._column_names = {'col_a': 'db_col_a'}

        df = pd.DataFrame({
            "col_a": ["row1col1", "row1 col 2", pd.NA],
            "col_b": ["ro w2c o l1", "row2 col 2", pd.NA],
            "col_c": [pd.NA, pd.NA, pd.NA],
        })
        mock_reader.get_chunk.side_effect = [df, StopIteration]

        result = self.os_importer._load_csv_chunk(mock_reader, "my src")

        mock_reader.get_chunk.assert_called_with(self.os_importer.CSV_CHUNK_SIZE)

        assert result is df
        assert self.os_importer._load_csv_chunk(mock_reader, "my src") is None

    def test_load_csv_parser_error(self):
        mock_reader = MagicMock(spec_set=TextFileReader)
        mock_reader.get_chunk.side_effect = ParserError()

        with self.assertRaisesRegex(GOBException, "CSV parsing exception: my src"):
            self.os_importer._load_csv_chunk(mock_reader, "my src")

    def test_load_csv_http_error(self):
        mock_reader = MagicMock(spec_set=TextFileReader)
        mock_reader.get_chunk.side_effect = [
            HTTPError("My url", 500, "err msg", "", None),
            http.client.IncompleteRead(partial=b"some bytes object")
        ] * 3

        with self.assertRaises(http.client.IncompleteRead):
            self.os_importer._load_csv_chunk(mock_reader, "my src")

    def test_process_chunk(self):
        df = pd.DataFrame({
            "col_a": ["row1col1", "row1 col 2", pd.NA],
            "col_b": ["ro w2c o l1", "row2 col 2", pd.NA],
            "col_c": [pd.NA, pd.NA, pd.NA],
        })
        self.os_importer._column_names = {"col_a": "col_a_db"}

        columns, data = self.os_importer._process_chunk(df)
        assert columns == ["col_a_db", "col_b", "col_c"]
        assert data == [
            ('row1col1', 'ro w2c o l1', None),
            ('row1 col 2', 'row2 col 2', None)
        ]

    @patch("gobprepare.importers.csv_importer.create_table_columnar_query")
    def test_create_destination_table(self, mock_create_table):
        columns = ["col_a", "col_b", "col_c", "col with SpAceS"]
        self.os_importer._create_destination_table(columns)

        mock_create_table.assert_called_with(
            self.os_importer._dst_datastore,
            self.config_objectstore['destination'],
            '"col_a" TEXT NULL, "col_b" TEXT NULL, "col_c" TEXT NULL, "col with SpAceS" TEXT NULL',
        )
        self.dst_datastore.execute.assert_called_with(mock_create_table.return_value)

    def test_import_data(self):
        data = [("1", "2"), ("3", "4"), ("5", "6")]
        self.os_importer._import_data(data)
        self.dst_datastore.write_rows.assert_called_with(self.os_importer._destination, data)

    @patch("gobprepare.importers.csv_importer.read_csv")
    def test__import_csv(self, mock_read_csv):
        columns = ["col_a", "col_b", "col_c"]
        data = [(1, 2, 3), (4, 4, 2), (2, 4, 5)]
        df = pd.DataFrame(data, columns=columns)

        self.os_importer._load_csv_chunk = MagicMock(side_effect=[df, None])
        self.os_importer._create_destination_table = MagicMock()
        self.os_importer._import_data = MagicMock(return_value=len(data))

        result = self.os_importer._import_csv("my source")

        assert result == 3
        assert self.os_importer._load_csv_chunk.call_count == 2
        self.os_importer._load_csv_chunk.assert_called_with(
            mock_read_csv.return_value.__enter__.return_value, "my source"
        )
        self.dst_datastore.execute.assert_called_with("ANALYZE schema.table")

        self.os_importer._create_destination_table.assert_called_with(columns)
        self.os_importer._import_data.assert_called_with(data)

        mock_read_csv.assert_called_with(
            "my source",
            index_col=False,
            keep_default_na=False,
            na_values="",
            sep=self.os_importer._separator,
            dtype=str,
            encoding=self.os_importer._encoding,
            iterator=True
        )

    @patch("gobprepare.importers.csv_importer._load_from_objectstore")
    @patch("gobprepare.importers.csv_importer.DatastoreFactory")
    @patch("gobprepare.importers.csv_importer.get_datastore_config")
    def test_import_csv(self, mock_get_config, mock_factory, mock_load_os):
        mock_factory.get_datastore.return_value = MagicMock(spec_set=ObjectDatastore)

        self.os_importer._import_csv = MagicMock()

        self.os_importer.import_csv()

        mock_get_config.assert_called_with(self.os_importer._objectstore)
        mock_factory.get_datastore.assert_called_with(mock_get_config.return_value, self.os_importer._read_config)

        mock_load_os.assert_called_with(mock_factory.get_datastore.return_value)
        self.os_importer._import_csv.assert_called_with(mock_load_os.return_value.__enter__.return_value)

        # exception on wrong datastore type
        mock_factory.get_datastore.return_value = type("OtherClass", (object, ), {})()
        with self.assertRaisesRegex(GOBException, "OtherClass"):
            self.os_importer.import_csv()

        # other sources (url)
        mock_load_os.reset_mock()
        self.url_importer._import_csv = MagicMock()
        self.url_importer.import_csv()
        self.url_importer._import_csv.assert_called_with(self.url_importer._source)
        mock_load_os.assert_not_called()

        # config exception
        with self.assertRaisesRegex(GOBException, "Incomplete config."):
            SqlCsvImporter(self.dst_datastore, {"destination": "dst"}).import_csv()

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

    def test_load_from_objectstore_not_found(self):
        mock_datastore = MagicMock(spec=ObjectDatastore)
        mock_datastore.query.return_value = iter([])
        mock_datastore.read_config = {"file_filter": "http://example.com/somefile.csv"}

        with self.assertRaisesRegex(GOBException, "http://example.com/somefile.csv"):
            with _load_from_objectstore(mock_datastore) as src_file:
                pass
