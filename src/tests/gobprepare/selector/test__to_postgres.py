from unittest import TestCase
from unittest.mock import patch, call, MagicMock

from gobprepare.selector._to_postgres import ToPostgresSelector


class TestToPostgresSelector(TestCase):

    def setUp(self) -> None:
        self.config = {
            "type": "select",
            "queries": [
                {
                    "query": ["SELECT SOMETHING", "FROM SOMEWHERE", "ETC"],
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
                },
            ]
        }
        self.selector = ToPostgresSelector()
        self.selector._dst_connection = MagicMock()

    @patch("gobprepare.selector._to_postgres.execute_postgresql_query")
    def test_create_destination_table(self, mock_execute):
        destination_table = self.config['queries'][0]['destination_table']

        self.selector._create_destination_table(destination_table)
        mock_execute.assert_called_with(self.selector._dst_connection,
                                        "CREATE TABLE dst.table (col_a VARCHAR(20) NULL,col_b TIMESTAMP NULL)")

    @patch("gobprepare.selector._to_postgres.Json")
    def test_prepare_row(self, mock_json):
        row = [
            {"key": "value"},
            {"key": "value"},
            {"key": "value"},
            {"key": "value"},
        ]
        columns = [
            {"type": "SOME_INNOCENT_TYPE"},
            {"type": "JSONB"},
            {"type": "SOME_INNOCENT_TYPE"},
            {"type": "SOME_INNOCENT_TYPE"},
        ]

        result = self.selector._prepare_row(row, columns)
        # Row 1 should be replaced with return value
        row[1] = mock_json.return_value
        self.assertEqual(row, result)

    @patch("gobprepare.selector._to_postgres.write_rows_to_postgresql")
    def test_write_rows(self, mock_write):
        table = "some_table"
        values = [[2, 4, 5], [2, 2, 0], [4, 4, 3]]
        self.selector._write_rows(table, values)
        mock_write.assert_called_with(self.selector._dst_connection, table, values)