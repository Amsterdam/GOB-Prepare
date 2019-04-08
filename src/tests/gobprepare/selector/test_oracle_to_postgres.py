from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobprepare.selector.oracle_to_postgres import OracleToPostgresSelector


class TestOracleToPostgresSelector(TestCase):

    def setUp(self) -> None:
        self.config = {
            "type": "select",
            "queries": [
                {
                    "query": [ "SELECT SOMETHING", "FROM SOMEWHERE", "ETC"],
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
                {
                    "query": [ "SELECT SOMETHING", "FROM SOMEWHERE_ELSE", "ETC"],
                    "destination_table": {
                        "name": "dst.table2",
                        "create": True,
                        "columns": [
                            {
                                "name": "col_a2",
                                "type": "VARCHAR(20)",
                            },
                            {
                                "name": "col_b2",
                                "type": "TIMESTAMP",
                            },
                        ],
                    },
                },
            ],
        }
        self.selector = OracleToPostgresSelector("oracle_connection", "postgres_connection", self.config)

    def test_init(self):
        self.assertEqual("oracle_connection", self.selector._src_connection)
        self.assertEqual("postgres_connection", self.selector._dst_connection)
        self.assertEqual(self.config, self.selector._config)
        self.assertEqual(self.config['queries'], self.selector.queries)

    def test_select(self):
        self.selector._select = MagicMock(side_effect=[483, 284])
        result = self.selector.select()
        self.assertEqual(483 + 284, result)

        self.selector._select.assert_has_calls([
            call("SELECT SOMETHING\nFROM SOMEWHERE\nETC", self.config['queries'][0]['destination_table']),
            call("SELECT SOMETHING\nFROM SOMEWHERE_ELSE\nETC", self.config['queries'][1]['destination_table']),
        ])

    @patch("gobprepare.selector.oracle_to_postgres.logger")
    @patch("gobprepare.selector.oracle_to_postgres.query_oracle")
    @patch("gobprepare.selector.oracle_to_postgres.write_rows_to_postgresql")
    def test__select(self, mock_write_rows, mock_query_oracle, mock_logger):
        result_cnt = 97
        query = "SOME QUERY"
        destination_table = self.config['queries'][0]['destination_table']

        self.selector.WRITE_BATCH_SIZE = 24
        self.selector._create_destination_table = MagicMock()

        # Create bogus data, matching with table definition
        mock_query_oracle.return_value = iter([{"col_a": str(i), "col_b": i} for i in range(result_cnt)])
        # Reset call count
        mock_logger.reset()

        result = self.selector._select(query, destination_table)

        self.assertEqual(result_cnt, result)
        self.selector._create_destination_table.assert_has_calls([call(destination_table)])
        self.assertEqual(5, mock_write_rows.call_count)
        mock_logger.info.assert_called_once()

    @patch("gobprepare.selector.oracle_to_postgres.logger")
    @patch("gobprepare.selector.oracle_to_postgres.query_oracle")
    @patch("gobprepare.selector.oracle_to_postgres.write_rows_to_postgresql")
    def test__select_no_create(self, mock_write_rows, mock_query_oracle, mock_logger):
        destination_table = self.config['queries'][0]['destination_table']
        del destination_table['create']

        self.selector._create_destination_table = MagicMock()
        self.selector._select("SOME QUERY", destination_table)
        # Assert that table is not created
        self.selector._create_destination_table.assert_not_called()

    @patch("gobprepare.selector.oracle_to_postgres.execute_postgresql_query")
    def test_create_destination_table(self, mock_execute):
        destination_table = self.config['queries'][0]['destination_table']

        self.selector._create_destination_table(destination_table)
        mock_execute.assert_called_with(self.selector._dst_connection,
                                        "CREATE TABLE dst.table (col_a VARCHAR(20) NULL,col_b TIMESTAMP NULL)")
