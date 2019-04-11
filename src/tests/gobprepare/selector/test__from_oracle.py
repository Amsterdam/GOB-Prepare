from unittest import TestCase
from unittest.mock import patch

from gobprepare.selector._from_oracle import FromOracleSelector


class TestFromOracleSelector(TestCase):

    def setUp(self) -> None:
        self.selector = FromOracleSelector()
        self.selector._src_connection = "oracle_connection"

    @patch("gobprepare.selector._from_oracle.query_oracle", return_value=['row_a', 'row_b'])
    def test_read_rows(self, mock_query_oracle):
        query = "some query"
        result = self.selector._read_rows(query)
        self.assertEqual(['row_a', 'row_b'], result)
        mock_query_oracle.assert_called_with(self.selector._src_connection, [query])