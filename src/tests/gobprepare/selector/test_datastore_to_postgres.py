from unittest.mock import MagicMock

from gobprepare.selector.datastore_to_postgres import DatastoreToPostgresSelector
from tests.gobprepare.selector.selector_testcase import SelectorTestCase


class TestDatastoreToPostgresSelector(SelectorTestCase):

    def test_init(self):
        src_conn = MagicMock()
        dst_conn = MagicMock()
        config = {
            "query": "some query",
            "query_src": "string",
            "destination_table": {},
        }

        selector = DatastoreToPostgresSelector(src_conn, dst_conn, config)
        self.assertEqual(src_conn, selector._src_datastore)
        self.assertEqual(dst_conn, selector._dst_datastore)
        self.assertEqual(config, selector._config)

        self.assertValidSelector(selector)
