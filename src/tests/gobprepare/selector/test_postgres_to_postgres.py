from unittest.mock import MagicMock

from gobprepare.selector.postgres_to_postgres import PostgresToPostgresSelector
from tests.gobprepare.selector.selector_testcase import SelectorTestCase


class TestPostgresToPostgresSelector(SelectorTestCase):

    def test_init(self):
        src_conn = MagicMock()
        dst_conn = MagicMock()
        config = {
            "query": "some query",
            "query_src": "string",
            "destination_table": {},
        }

        selector = PostgresToPostgresSelector(src_conn, dst_conn, config)
        self.assertEqual(src_conn, selector._src_connection)
        self.assertEqual(dst_conn, selector._dst_connection)
        self.assertEqual(config, selector._config)

        self.assertValidSelector(selector)
