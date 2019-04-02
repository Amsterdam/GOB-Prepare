from unittest import TestCase
from unittest.mock import patch

from gobcore.exceptions import GOBException

from gobprepare.config import get_url, ORACLE_DRIVER, get_database_config


class TestConfig(TestCase):

    def setUp(self):
        self.config = {
            'drivername': ORACLE_DRIVER,
            'username': "username",
            'password': "password",
            'host': "host",
            'port': 1234,
            'database': 'x.y.z'
        }

    def test_get_url_oracle(self):
        self.assertEqual(get_url(self.config), "oracle+cx_oracle://username:password@host:1234/?service_name=x.y.z")

    def test_get_url_not_oracle(self):
        self.config['drivername'] = 'somedriver'
        self.assertEqual(str(get_url(self.config)), "somedriver://username:password@host:1234/x.y.z")

    @patch("gobprepare.config.get_url", return_value="dburl")
    def test_get_database_config(self, mock_get_url):
        available = [
            'Neuron',
            'GOBPrepare',
        ]

        for config in available:
            result = get_database_config(config)
            self.assertEqual(result['name'], config)
            self.assertEqual(result['url'], 'dburl')

    def test_get_nonexistent_database_config(self):
        with self.assertRaises(GOBException):
            get_database_config("nonexistent")