from unittest import TestCase
from unittest.mock import call, patch, MagicMock, mock_open

from gobcore.exceptions import GOBException
from gobprepare.selector._selector import Selector


class TestSelector(TestCase):

    def setUp(self) -> None:
        self.config = {
            "type": "select",
            "query": ["SELECT SOMETHING", "FROM SOMEWHERE", "ETC"],
            "query_src": "string",
            "destination_table": {
                "name": "dst.table",
                "create": True,
                "columns": [
                    {
                        "name": "col_a",
                        "type": "VARCHAR(20)",
                    },
                    {
                        "name": "col_b",
                        "type": "TIMESTAMP",
                    },
                ],
            },
        }
        self.selector = Selector("oracle_connection", "postgres_connection", self.config)

    def test_init(self):
        self.assertEqual("oracle_connection", self.selector._src_connection)
        self.assertEqual("postgres_connection", self.selector._dst_connection)
        self.assertEqual(self.config, self.selector._config)
        self.assertEqual("\n".join(self.config['query']), self.selector.query)

    def test__get_query_string_type(self):
        config = {
            "query_src": "string",
            "query": "SELECT SOMETHING FROM SOMEWHERE WHERE SOMETHING IS TRUE",
        }

        result = self.selector._get_query(config)
        self.assertEqual(config["query"], result)

    def test__get_query_string_as_list_type(self):
        config = {
            "query_src": "string",
            "query": ["SELECT", "SOMETHING", "FROM", "SOMEWHERE"],
        }
        result = self.selector._get_query(config)
        self.assertEqual("\n".join(config['query']), result)

    @patch("builtins.open", new_callable=mock_open, read_data="the query")
    def test__get_query_file_type(self, mock_file_open):
        config = {
            "query_src": "file",
            "query": "some/file/path.sql",
        }
        result = self.selector._get_query(config)
        mock_file_open.assert_called_with(config['query'])
        self.assertEqual("the query", result)

    def test__get_query_invalid_type(self):
        config = {
            "query_src": "invalid"
        }
        with self.assertRaises(NotImplementedError):
            self.selector._get_query(config)

    @patch("gobprepare.selector._selector.logger")
    def test_select(self, mock_logger):
        result_cnt = 97
        destination_table = self.config['destination_table']

        self.selector.WRITE_BATCH_SIZE = 24
        self.selector._create_destination_table = MagicMock()
        self.selector._read_rows = MagicMock()
        self.selector._write_rows = MagicMock()

        # Mock values list. Important that returned length is the same as length of input generator x.
        self.selector._values_list = lambda x, y: [[] for _ in x]

        # Create bogus data, matching with table definition
        self.selector._read_rows.return_value = iter([{"col_a": str(i), "col_b": i} for i in range(result_cnt)])

        # Reset call count
        mock_logger.reset()

        result = self.selector.select()

        self.assertEqual(result_cnt, result)
        self.selector._create_destination_table.assert_has_calls([call(destination_table)])
        self.assertEqual(5, self.selector._write_rows.call_count)
        mock_logger.info.assert_called_once()

    @patch("gobprepare.selector._selector.logger")
    def test_select_no_create(self, mock_logger):
        destination_table = self.config['destination_table']
        del destination_table['create']

        self.selector._create_destination_table = MagicMock()
        self.selector._read_rows = MagicMock()
        self.selector._write_rows = MagicMock()
        self.selector.select()
        # Assert that table is not created
        self.selector._create_destination_table.assert_not_called()

    def test_values_list(self):
        self.selector._prepare_row = lambda x, y: x  # return rowvals as is
        rows = [
            {"col_a": 8, "col_b": 2, "col_c": 7},
            {"col_b": 2, "col_c": 5, "col_a": 0},
            {"col_c": 2, "col_b": 4, "col_a": 6},
            {"col_a": 4, "col_c": 8, "col_b": 2},
        ]
        cols = [
            {"name": "col_a"},
            {"name": "col_b"},
            {"name": "col_c"}
        ]

        # Expect values of 'rows' in the order of 'cols'
        expected_result = [
            [8, 2, 7],
            [0, 2, 5],
            [6, 4, 2],
            [4, 2, 8]
        ]
        self.assertEqual(expected_result, self.selector._values_list(rows, cols))

    def test_values_list_missing_column_exception(self):
        self.selector._prepare_row = lambda x, y: x  # return rowvals as is
        rows = [
            {"col_a": 8, "col_b": 2, "col_c": 7},
            {"col_b": 2, "col_c": 5, "col_a": 0},
            {"col_c": 2, "col_b": 4},
            {"col_a": 4, "col_c": 8, "col_b": 2},
        ]
        cols = [
            {"name": "col_a"},
            {"name": "col_b"},
            {"name": "col_c"}
        ]

        with self.assertRaisesRegex(GOBException, "Missing column"):
            self.selector._values_list(rows, cols)

    def test_values_list_missing_column_allowed(self):
        self.selector._prepare_row = lambda x, y: x  # return rowvals as is
        rows = [
            {"col_a": 8, "col_b": 2, "col_c": 7},
            {"col_b": 2, "col_c": 5, "col_a": 0},
            {"col_c": 2, "col_b": 4},
            {"col_a": 4, "col_b": 2},
        ]
        cols = [
            {"name": "col_a"},
            {"name": "col_b"},
            {"name": "col_c"}
        ]

        self.selector.ignore_missing = True

        # Expect values of 'rows' in the order of 'cols' with missing values set to None
        expected_result = [
            [8, 2, 7],
            [0, 2, 5],
            [None, 4, 2],
            [4, 2, None]
        ]
        self.assertEqual(expected_result, self.selector._values_list(rows, cols))
