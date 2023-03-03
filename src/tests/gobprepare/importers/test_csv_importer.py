from unittest import TestCase
from unittest.mock import MagicMock, patch

from pandas.errors import ParserError
from urllib.error import HTTPError
from gobcore.exceptions import GOBException
from gobprepare.importers.csv_importer import SqlCsvImporter, CONTAINER_BASE, ObjectDatastore


@patch("gobprepare.importers.csv_importer.SqlCsvImporter._load_from_objectstore", MagicMock())
class TestSqlCsvImporter(TestCase):
    class MockPandasDataFrame():
        class MockRow():
            def __init__(self, vals):
                self.vals = vals

            def tolist(self):
                return self.vals

        rows = [
            ['row1col1', 'row1 col 2', 'row1col 3'],
            ['ro w2c o l1', 'row2 col 2', 'r o w 2 c o l 3'],
            ['', '', ''],
            ['row3 col1', 'r o w3 col 2', 'ro w 3 c ol 3'],
            ['', '', ''],
            ['', '', ''],
            ['', '', ''],
        ]
        cols = {
            "col_a": [row[0] for row in rows],
            "col_b": [row[1] for row in rows],
            "col_c": [row[2] for row in rows],
        }
        columns = cols.keys()

        def __getitem__(self, item):
            return self.cols[item]

        def iterrows(self):
            return [
                (1, self.MockRow(self.rows[0])),
                (2, self.MockRow(self.rows[1])),
                (3, self.MockRow(self.rows[2])),
                (3, self.MockRow(self.rows[3])),
                (3, self.MockRow(self.rows[4])),
                (3, self.MockRow(self.rows[5])),
                (3, self.MockRow(self.rows[6])),
            ]

    def _setup_importer(self):
        self.config = {
            "type": "import_csv",
            "read_config": {
                "file_filter": "http://example.com/somefile.csv",
            },
            "destination": "schema.table",
            "objectstore": "TheObjectstore"
        }

        self.dst_datastore = MagicMock()
        self.importer = SqlCsvImporter(self.dst_datastore, self.config)

    def test_init(self):
        self._setup_importer()
        self.assertEqual(self.dst_datastore, self.importer._dst_datastore)
        self.assertEqual(self.importer._load_from_objectstore.return_value, self.importer._source)
        self.assertEqual(self.config['destination'], self.importer._destination)
        self.assertEqual(',', self.importer._separator)
        self.assertEqual({}, self.importer._column_names)
        self.assertEqual('utf-8', self.importer._encoding)

        self.importer._load_from_objectstore.assert_called_with("TheObjectstore", self.config['read_config'])

    def test_init_source(self):
        config = {
            "type": "import_csv",
            "destination": "schema.table",
            "source": "the source",
        }
        self.importer = SqlCsvImporter(MagicMock(), config)
        self.assertEqual("the source", self.importer._source)

    def test_init_no_objectstore_or_source(self):
        config = {"destination": "dst"}

        with self.assertRaises(GOBException):
            SqlCsvImporter(MagicMock(), config)

    def test_init_non_defaults(self):
        self._setup_importer()
        self.config['column_names'] = {'csv_column': 'db_column'}
        self.config['separator'] = ';'
        self.config['encoding'] = 'other-encoding'

        importer = SqlCsvImporter(self.dst_datastore, self.config)
        self.assertEqual(';', importer._separator)
        self.assertEqual({'csv_column': 'db_column'}, importer._column_names)
        self.assertEqual('other-encoding', importer._encoding)

    def test_empty_row(self):
        self._setup_importer()
        class MockPandasList:
            def __init__(self, lst: list):
                self.lst = lst

            def tolist(self):
                return self.lst

        testcases = (
            (MockPandasList([]), True),
            (MockPandasList([1, 2]), False),
            (MockPandasList(['', '']), True),
            (MockPandasList(['', '2']), False),
            (MockPandasList(['', 0]), False),
        )

        for input, output in testcases:
            self.assertEqual(output, self.importer._is_empty_row(input))

    @patch("gobprepare.importers.csv_importer.read_csv")
    def test_load_csv(self, mock_read_csv):
        self._setup_importer()
        mock_pandas = self.MockPandasDataFrame()
        mock_read_csv.return_value = mock_pandas

        # col_a will be replaced by db_col_a
        self.importer._column_names = {
            'col_a': 'db_col_a'
        }
        self.importer._encoding = 'the-encoding'

        result = self.importer._load_csv()

        expected_result = {
            "columns": [
                {"name": "db_col_a", "max_length": max([len(i) for i in mock_pandas.cols['col_a']])},
                {"name": "col_b", "max_length": max([len(i) for i in mock_pandas.cols['col_b']])},
                {"name": "col_c", "max_length": max([len(i) for i in mock_pandas.cols['col_c']])},
            ],
            "data": [
                # Empty rows should not be returned
                [str(i) for i in mock_pandas.rows[0]],
                [str(i) for i in mock_pandas.rows[1]],
                [str(i) for i in mock_pandas.rows[3]],
            ]
        }
        self.assertEqual(expected_result, result)
        mock_read_csv.assert_called_with(self.importer._source, keep_default_na=False, sep=self.importer._separator, encoding='the-encoding', dtype=str)


    @patch("gobprepare.importers.csv_importer.read_csv")
    def test_load_csv_parser_error(self, mock_read_csv):
        self._setup_importer()
        mock_read_csv.side_effect = ParserError()
        self.importer._source = 'the source'

        with self.assertRaisesRegex(GOBException, self.importer._source):
            self.importer._load_csv()

    @patch("gobprepare.importers.csv_importer.read_csv")
    def test_load_csv_http_error(self, mock_read_csv):
        self._setup_importer()
        mock_read_csv.side_effect = HTTPError("", "", "", "", "")

        self.importer.WAIT_RETRY = 0
        self.importer._source = 'the source'

        with self.assertRaisesRegex(GOBException, self.importer._source):
            self.importer._load_csv()

    @patch("gobprepare.importers.csv_importer.tempfile.gettempdir", lambda: '/the_tmp_dir')
    @patch("gobprepare.importers.csv_importer.os.makedirs")
    def test_tmp_filename(self, mock_makedirs):
        self._setup_importer()
        objectstore_filename = 'the/file/on_objectstore/dir/file.ext'
        expected_tmp_filename = '/the_tmp_dir/the/file/on_objectstore/dir/file.ext'
        self.assertEqual(expected_tmp_filename, self.importer._tmp_filename(objectstore_filename))

        mock_makedirs.assert_called_with('/the_tmp_dir/the/file/on_objectstore/dir', exist_ok=True)

    @patch("gobprepare.importers.csv_importer.create_table_columnar_query")
    def test_create_destination_table(self, mock_create_table):
        self._setup_importer()
        columns = [
            {"max_length": 20, "name": "col_a"},
            {"max_length": 19, "name": "col_b"},
            {"max_length": 8, "name": "col_c"},
            {"max_length": 8, "name": "col with SpAceS"},
        ]
        self.importer._create_destination_table(columns)
        mock_create_table.assert_called_with(
            self.importer._dst_datastore,
            self.config['destination'],
            '"col_a" VARCHAR(25) NULL,"col_b" VARCHAR(24) NULL,'
            '"col_c" VARCHAR(13) NULL,"col with SpAceS" VARCHAR(13) NULL',
        )
        self.dst_datastore.execute.assert_called_with(mock_create_table.return_value)

    def test_import_data(self):
        self._setup_importer()
        data = [[1, 2, 3], [4, 4, 2], [2, 4, 5]]
        self.importer._import_data(data)
        self.dst_datastore.write_rows.assert_called_with(self.importer._destination, data)

    def test_import_csv(self):
        self._setup_importer()
        data = {
            "columns": [
                {"max_length": 20, "name": "col_a"},
                {"max_length": 19, "name": "col_b"},
                {"max_length": 8, "name": "col_c"},
            ],
            "data": [[1, 2, 3], [4, 4, 2], [2, 4, 5]],
        }
        self.importer._load_csv = MagicMock(return_value=data)
        self.importer._create_destination_table = MagicMock()
        self.importer._import_data = MagicMock()

        result = self.importer.import_csv()
        self.assertEqual(3, result)
        self.importer._load_csv.assert_called_once()
        self.importer._create_destination_table.assert_called_with(data['columns'])
        self.importer._import_data.assert_called_with(data['data'])


class TestSqlCsvImporterLoad(TestCase):

    @patch("gobprepare.importers.csv_importer.SqlCsvImporter._tmp_filename")
    @patch("gobprepare.importers.csv_importer.DatastoreFactory")
    @patch("gobprepare.importers.csv_importer.get_datastore_config")
    @patch("builtins.open")
    def test_load_from_objectstore(self, mock_open, mock_get_config, mock_factory, mock_tmp_filename):
        config = {
            "type": "import_csv",
            "read_config": {
                "file_filter": "http://example.com/somefile.csv",
            },
            "destination": "schema.table",
            "objectstore": "TheObjectstore"
        }
        dst_datastore = MagicMock()
        mock_connection = MagicMock()
        mock_connection.get_object.return_value = [{}, 'the object data']
        mock_factory.get_datastore.return_value = MagicMock(spec=ObjectDatastore)
        mock_factory.get_datastore.return_value.container_name = CONTAINER_BASE
        mock_factory.get_datastore.return_value.connection = mock_connection
        mock_factory.get_datastore.return_value.query.return_value = iter([{'name': 'file/location/on/objectstore.ext'}])

        importer = SqlCsvImporter(dst_datastore, config)
        importer._tmp_filename = MagicMock()

        mock_connection.get_object.assert_called_with(CONTAINER_BASE, 'file/location/on/objectstore.ext')
        mock_get_config.assert_called_with('TheObjectstore')
        mock_factory.get_datastore.assert_called_with(mock_get_config.return_value, config['read_config'])

        mock_open.return_value.__enter__.return_value.write.assert_called_with('the object data')
        self.assertEqual(mock_tmp_filename.return_value, importer._source)

    @patch("gobprepare.importers.csv_importer.DatastoreFactory")
    @patch("gobprepare.importers.csv_importer.get_datastore_config", MagicMock())
    def test_load_from_objectstore_not_found(self, mock_factory):
        mock_factory.get_datastore.return_value = MagicMock()
        config = {
            "type": "import_csv",
            "read_config": {
                "file_filter": "http://example.com/somefile.csv",
            },
            "destination": "schema.table",
            "objectstore": "TheObjectstore"
        }

        mock_factory.get_datastore.return_value = MagicMock(spec=ObjectDatastore)
        mock_factory.get_datastore.return_value.query.return_value = iter([])

        with self.assertRaises(GOBException):
            SqlCsvImporter(MagicMock(), config)

    @patch("gobprepare.importers.csv_importer.DatastoreFactory")
    @patch("gobprepare.importers.csv_importer.get_datastore_config", MagicMock())
    def test_load_from_objectstore_invalid_store(self, mock_factory):
        mock_factory.get_datastore.return_value = MagicMock()
        config = {
            "type": "import_csv",
            "read_config": {
                "file_filter": "http://example.com/somefile.csv",
            },
            "destination": "schema.table",
            "objectstore": "TheObjectstore"
        }

        with self.assertRaisesRegex(AssertionError, "Expected Objectstore"):
            SqlCsvImporter(MagicMock(), config)

