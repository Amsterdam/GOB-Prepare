from unittest import TestCase
from unittest.mock import MagicMock, patch
import pandas as pd
from swiftclient import Connection

from gobcore.datastore.sql import SqlDatastore
from gobcore.datastore.postgres import PostgresDatastore
from gobcore.exceptions import GOBException
from gobprepare.importers.dump_importer import SqlDumpImporter, ObjectDatastore, _load_from_objectstore
from gobprepare.importers.typing import SqlDumpImporterConfig

class TestSqlDumpImporter(TestCase):
    """Test SqlDumpImporter."""

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
                    "OLD_SCHEMA_NAME": "NEW_SCHEMA_NAME",
                    "pattern_2": "replace_2"
                },
                "comments_regexp": "(?m)^\\s*--.*$",
                "split_regexp": ";\\n|^\\.\n'",
                "data_delimiter_regexp": "\\.\n",
                "copy_query_regex": "COPY.*FROM stdin;"
            },
            "destination": "schema.table",
            "objectstore": "TheObjectstore"
        }
        self.query = "CREATE TABLE OLD_SCHEMA_NAME.someTable (id numeric(18,0) NOT NULL, name character varying(20))"
        self.copy_query = "COPY schema.sometable (colomn1, colomn 2, colomn3, colomn4) FROM stdin;"
        self.data_input = "value1	value2	value3	value4\nvalue5	value6	value7	value8\n"

        self.dst_datastore = MagicMock(spec_set=PostgresDatastore)
        self.os_importer = SqlDumpImporter(self.dst_datastore, self.config_objectstore)

    def test_init(self):
        assert self.os_importer._dst_datastore == self.dst_datastore
        assert self.os_importer._encoding == "utf-8"
        assert self.os_importer._read_config == self.config_objectstore["read_config"]
        assert self.os_importer._objectstore == self.config_objectstore["objectstore"]

    @patch("gobprepare.importers.dump_importer.logger")
    @patch("gobprepare.importers.dump_importer.tempfile")
    def test_load_from_objectstore(self, mock_tmpfile, mock_logger):
        mock_datastore = MagicMock(spec=ObjectDatastore)
        mock_datastore.container_name = "my container base"
        mock_datastore.query.return_value = iter([{"name": "file/location/on/objectstore.ext"}])

        mock_datastore.connection = MagicMock(spec=Connection)
        mock_datastore.connection.get_object.return_value = {}, ["the dump object"]

        mock_tmpfile.NamedTemporaryFile.return_value.__enter__.return_value.name = "my src file"

        with _load_from_objectstore(mock_datastore) as src_file:
            assert src_file == "my src file"
        mock_logger.info.assert_called_once()

        mock_tmpfile.NamedTemporaryFile.assert_called_with(mode="wb", suffix=".gz")
        mock_tmpfile.NamedTemporaryFile.return_value.__enter__.return_value.write.assert_called_with("the dump object")

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
        mock_datastore.read_config = {"file_filter": "http://example.com/somefile.sql.gz"}

        with self.assertRaisesRegex(GOBException, "http://example.com/somefile.sql.gz"):
            with _load_from_objectstore(mock_datastore) as src_file:
                pass

    @patch("gobprepare.importers.dump_importer._load_from_objectstore")
    @patch("gobprepare.importers.dump_importer.DatastoreFactory")
    @patch("gobprepare.importers.dump_importer.get_datastore_config")
    def test_import_dump(self, mock_get_config, mock_factory, mock_load_os):
        mock_factory.get_datastore.return_value = MagicMock(spec_set=ObjectDatastore)
        self.os_importer._read_config == self.config_objectstore["read_config"]
        self.os_importer._import_dump = MagicMock()

        self.os_importer.import_dump()

        mock_get_config.assert_called_with(self.os_importer._objectstore)
        # mock_factory.get_datastore.assert_called_with(mock_get_config.return_value, self.config_objectstore["read_config"])

        mock_load_os.assert_called_with(mock_factory.get_datastore.return_value)
        self.os_importer._import_dump.assert_called_with(mock_load_os.return_value.__enter__.return_value)

        # exception on wrong datastore type
        mock_factory.get_datastore.return_value = type("OtherClass", (object, ), {})()
        with self.assertRaisesRegex(GOBException, "OtherClass"):
            self.os_importer.import_dump()

        # config exception
        with self.assertRaisesRegex(GOBException, "Incomplete config. Expecting key 'objectstore'"):
            SqlDumpImporter(self.dst_datastore, {"read_config": {
                "file_filter": "http://example.com/somefile.sql.gz"}}).import_dump()

    @patch("gobprepare.importers.dump_importer.logger")
    @patch("gobprepare.importers.dump_importer.GzipFile")
    def test__import_dump(self, mock_GzipFile, mock_logger):
        file_path = "path/to/somefile.sql.gz"
        expected_content = "Content of the file"

        self.os_importer._extract_sql_queries = MagicMock(return_value=self.query)
        self.os_importer._process_and_execute_queries = MagicMock()

        mock_decompressed_file = mock_GzipFile.return_value
        mock_decompressed_file.read.return_value.decode.return_value = expected_content

        result = self.os_importer._import_dump(file_path)
        self.assertEqual(result, "somefile.sql.gz")
        mock_GzipFile.assert_called_with(file_path, 'r')
        mock_logger.info.assert_called_once()

    def test_extract_sql_queries(self):
        dump_with_comment = """
        --
        -- this is a comment line; With extra comment; Schema: igp_pgplup_cmg_owner; Owner: igp_pgplup_cmg_owner
        --
        CREATE TABLE OLD_SCHEMA_NAME.someTable (id numeric(18,0) NOT NULL, name character varying(20));
        """
        expected_query = "\n        CREATE TABLE OLD_SCHEMA_NAME.someTable (id numeric(18,0) NOT NULL, name character varying(20));\n        "

        sql_queries = self.os_importer._extract_sql_queries(dump_with_comment)
        result = next(query for query in sql_queries)

        self.assertEqual(result, expected_query)

    def test_perform_query_substituions(self):
        expected_query = "CREATE TABLE NEW_SCHEMA_NAME.someTable (id numeric(18,0) NOT NULL, name character varying(20));"

        result = self.os_importer._perform_query_substitutions(self.query)
        self.assertEqual(result, expected_query)

    @patch("gobprepare.importers.dump_importer.StringIO")
    def test_instert_data(self, mock_io):
        test_data ="value1	value2	value3	value4\nvalue5	value6	value7	value8\n\\.\n"
        self.dst_datastore.copy_from_stdin.return_value = None
        self.os_importer._instert_data(self.copy_query, test_data)

        self.dst_datastore.copy_from_stdin.assert_called_with(self.copy_query, mock_io.return_value)
        mock_io.assert_called_with(self.data_input)

    @patch("gobprepare.importers.dump_importer.StringIO")
    def test_process_and_execute_queries(self, mock_io):
        queries = [
            "ANALYZE schema.table",
            "COPY schema.sometable (colomn1, colomn 2, colomn3, colomn4) FROM stdin",
            "value1	value2	value3	value4\nvalue5	value6	value7	value8\n\\.\n"]
        other_query = "ANALYZE schema.table;"

        self.dst_datastore.copy_from_stdin.return_value = None
        self.os_importer._process_and_execute_queries(queries)

        self.dst_datastore.copy_from_stdin.assert_called_with(self.copy_query, mock_io.return_value)
        mock_io.assert_called_with(self.data_input)
        self.dst_datastore.copy_from_stdin.assert_called_once()
        self.dst_datastore.execute.assert_called_with(other_query)