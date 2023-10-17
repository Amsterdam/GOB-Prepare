from unittest import TestCase
from unittest.mock import patch

from gobprepare.utils.postgres import create_table_columnar_query, create_table_columnar_as_query, brp_build_date_json


class MockPostgresDatastore:

    def __init__(self, version: str, citus_enabled: bool):
        self.version = version
        self.citus_enabled = citus_enabled

    def get_version(self):
        return self.version

    def is_extension_enabled(self, ext: str):
        if ext == "citus":
            return self.citus_enabled
        raise Exception("Mock only implemented for citus")


@patch("gobprepare.utils.postgres.logger.warning")
class TestPostgresUtils(TestCase):

    def test_create_columnar_as_query(self, mock_logger_warning):
        inputs = [
            ("11.0.1", True),
            ("12.1.0", False),
            ("11.2.1", False),
        ]

        for version, citus_enabled in inputs:
            mock_logger_warning.reset_mock()
            datastore = MockPostgresDatastore(version, citus_enabled)

            result = create_table_columnar_as_query(datastore, "schema.tablename", "SELECT * FROM somewhere")

            self.assertEqual("CREATE TABLE schema.tablename AS SELECT * FROM somewhere", result)
            mock_logger_warning.assert_called_once()

        inputs = [
            ("12.0.1", True),
            ("13.1.1", True),
        ]

        for version, citus_enabled in inputs:
            mock_logger_warning.reset_mock()
            datastore = MockPostgresDatastore(version, citus_enabled)

            result = create_table_columnar_as_query(datastore, "schema.tablename", "SELECT * FROM somewhere")

            self.assertEqual("CREATE TABLE schema.tablename USING columnar AS SELECT * FROM somewhere", result)
            mock_logger_warning.assert_not_called()

    def test_create_columnar_query(self, mock_logger_warning):
        inputs = [
            ("11.0.1", True),
            ("12.1.0", False),
            ("11.2.1", False),
        ]

        for version, citus_enabled in inputs:
            mock_logger_warning.reset_mock()
            datastore = MockPostgresDatastore(version, citus_enabled)

            result = create_table_columnar_query(datastore, "schema.tablename", "id int, text varchar")

            self.assertEqual("CREATE TABLE schema.tablename (id int, text varchar)", result)
            mock_logger_warning.assert_called_once()

        inputs = [
            ("12.0.1", True),
            ("13.1.1", True),
        ]

        for version, citus_enabled in inputs:
            mock_logger_warning.reset_mock()
            datastore = MockPostgresDatastore(version, citus_enabled)

            result = create_table_columnar_query(datastore, "schema.tablename", "id int, text varchar")

            self.assertEqual("CREATE TABLE schema.tablename (id int, text varchar) USING columnar", result)
            mock_logger_warning.assert_not_called()

