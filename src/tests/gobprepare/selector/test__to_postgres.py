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

    @patch("gobprepare.selector._to_postgres.write_rows_to_postgresql")
    def test_write_rows(self, mock_write):
        table = "some_table"
        values = [[2, 4, 5], [2, 2, 0], [4, 4, 3]]
        self.selector._write_rows(table, values)
        mock_write.assert_called_with(self.selector._dst_connection, table, values)
