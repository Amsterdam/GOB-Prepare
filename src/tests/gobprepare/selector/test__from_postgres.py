from unittest import TestCase
from unittest.mock import patch

from gobprepare.selector._from_postgres import FromPostgresSelector


class TestFromPostgresSelector(TestCase):

    def setUp(self) -> None:
        self.selector = FromPostgresSelector()
        self.selector._src_connection = "postgres_connection"

    @patch("gobprepare.selector._from_postgres.query_postgresql", return_value=['row_a', 'row_b'])
    def test_read_rows(self, mock_query_postgresql):
        query = "some query"
        result = self.selector._read_rows(query)
        self.assertEqual(['row_a', 'row_b'], result)
        mock_query_postgresql.assert_called_with(self.selector._src_connection, query)