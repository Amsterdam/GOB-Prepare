from unittest import TestCase
from unittest.mock import MagicMock, patch

from pandas.errors import ParserError
from urllib.error import HTTPError
from gobcore.exceptions import GOBException
from gobprepare.csv_importer.postgres import PostgresCsvImporter


class TestPostgresCsvImporter(TestCase):
    class MockPandasDataFrame():
        class MockRow():
            def __init__(self, vals):
                self.vals = vals

            def tolist(self):
                return self.vals

        rows = [
            ['row1col1', 'row1 col 2', 'row1col 3'],
            ['ro w2c o l1', 'row2 col 2', 'r o w 2 c o l 3'],
            ['row3 col1', 'r o w3 col 2', 'ro w 3 c ol 3'],
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
            ]

    def setUp(self) -> None:
        self.config = {
            "type": "import_csv",
            "source": "http://example.com/somefile.csv",
            "destination": "schema.table"
        }
        self.dst_connection = MagicMock()
        self.importer = PostgresCsvImporter(self.dst_connection, self.config)

    def test_init(self):
        self.assertEqual(self.dst_connection, self.importer._dst_connection)
        self.assertEqual(self.config['source'], self.importer._source)
        self.assertEqual(self.config['destination'], self.importer._destination)

    @patch("gobprepare.csv_importer.postgres.read_csv")
    def test_load_csv(self, mock_read_csv):
        mock_pandas = self.MockPandasDataFrame()
        mock_read_csv.return_value = mock_pandas

        result = self.importer._load_csv()

        expected_result = {
            "columns": [
                {"name": "col_a", "max_length": max([len(i) for i in mock_pandas.cols['col_a']])},
                {"name": "col_b", "max_length": max([len(i) for i in mock_pandas.cols['col_b']])},
                {"name": "col_c", "max_length": max([len(i) for i in mock_pandas.cols['col_c']])},
            ],
            "data": [
                [str(i) for i in mock_pandas.rows[0]],
                [str(i) for i in mock_pandas.rows[1]],
                [str(i) for i in mock_pandas.rows[2]],
            ]
        }
        self.assertEqual(expected_result, result)
        mock_read_csv.assert_called_with(self.importer._source, keep_default_na=False)

    @patch("gobprepare.csv_importer.postgres.read_csv")
    def test_load_csv_parser_error(self, mock_read_csv):
        mock_read_csv.side_effect = ParserError()

        with self.assertRaisesRegex(GOBException, self.config['source']):
            self.importer._load_csv()

    @patch("gobprepare.csv_importer.postgres.read_csv")
    def test_load_csv_http_error(self, mock_read_csv):
        mock_read_csv.side_effect = HTTPError("", "", "", "", "")

        self.importer.WAIT_RETRY = 0

        with self.assertRaisesRegex(GOBException, self.config['source']):
            self.importer._load_csv()

    @patch("gobprepare.csv_importer.postgres.execute_postgresql_query")
    def test_create_destination_table(self, mock_execute):
        columns = [
            {"max_length": 20, "name": "col_a"},
            {"max_length": 19, "name": "col_b"},
            {"max_length": 8, "name": "col_c"},
        ]
        expected_query = f"CREATE TABLE {self.config['destination']} " \
            f"(col_a VARCHAR(25) NULL,col_b VARCHAR(24) NULL,col_c VARCHAR(13) NULL)"
        self.importer._create_destination_table(columns)
        mock_execute.assert_called_with(self.importer._dst_connection, expected_query)

    @patch("gobprepare.csv_importer.postgres.write_rows_to_postgresql")
    def test_import_data(self, mock_write):
        data = [[1, 2, 3], [4, 4, 2], [2, 4, 5]]
        self.importer._import_data(data)
        mock_write.assert_called_with(self.importer._dst_connection, self.importer._destination, data)

    def test_import_csv(self):
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
