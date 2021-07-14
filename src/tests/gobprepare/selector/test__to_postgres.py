from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobprepare.selector._to_postgres import ToPostgresSelector
from gobprepare.utils.exceptions import DuplicateTableError


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
        self.selector._dst_datastore = MagicMock()

    def test_create_destination_table(self):
        destination_table = self.config['queries'][0]['destination_table']

        self.selector._create_destination_table(destination_table)
        self.selector._dst_datastore.execute.assert_called_with(
            "CREATE TABLE dst.table (col_a VARCHAR(20) NULL,col_b TIMESTAMP NULL)"
        )

    def test_create_destination_table_error(self):
        destination_table = self.config['queries'][0]['destination_table']
        self.selector._dst_datastore.list_tables_for_schema.return_value = ['table']

        with self.assertRaises(DuplicateTableError, msg="Table already exists: table (dst)"):
            self.selector._create_destination_table(destination_table)

    @patch("gobprepare.selector._to_postgres.Json")
    def test_prepare_row(self, mock_json):
        row = [
            {"key": "value"},
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
            {"type": "JSON"},
        ]

        result = self.selector._prepare_row(row, columns)
        # Rows 1 and 4 should be replaced with return value
        row[1] = row[4] = mock_json.return_value
        self.assertEqual(row, result)

    def test_write_rows(self):
        table = "some_table"
        values = [[2, 4, 5], [2, 2, 0], [4, 4, 3]]
        self.selector._write_rows(table, values)
        self.selector._dst_datastore.write_rows.assert_called_with(table, values)
