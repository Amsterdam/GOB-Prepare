from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobprepare.prepare_client import PrepareClient
from tests import fixtures


@patch('gobprepare.prepare_client.logger')
@patch("gobprepare.prepare_client.OracleToPostgresCloner", autospec=True)
class TestPrepareClient(TestCase):

    def setUp(self):
        self.mock_dataset = {
            'version': '0.1',
            'source': {
                'application': fixtures.random_string(),
                'type': 'oracle',
                'schema': fixtures.random_string()
            },
            'destination': {
                'application': fixtures.random_string(),
                'schema': fixtures.random_string()
            },
            'action': {
                'type': 'clone',
                'mask': {
                    "sometable": {
                        "somecolumn": "mask"
                    }
                }
            }
        }

        self.mock_msg = {
            'header': {
                "someheader": "value",
            },
        }

    def test_init(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        # Expect a process_id is created
        self.assertTrue(prepare_client.process_id)
        self.assertEqual(self.mock_msg['header'], prepare_client.header)

        # Assert the logger is configured and called
        mock_logger.set_name.assert_called()
        mock_logger.set_default_args.assert_called()
        mock_logger.info.assert_called()

    @patch("gobprepare.prepare_client.connect_to_oracle", return_value=["connection", "user"])
    @patch("gobprepare.prepare_client.connect_to_postgresql", return_value=["connection", "user"])
    @patch("gobprepare.prepare_client.get_database_config", return_value={})
    def test_connect(self, mock_database_config, mock_connect_postgres, mock_connect_oracle, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        # Reset counter
        mock_logger.info.reset_mock()
        prepare_client.connect()

        mock_connect_oracle.assert_called_once()
        mock_connect_postgres.assert_called_once()
        self.assertEqual(2, mock_logger.info.call_count)

    def test_disconnect(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        src_mock = MagicMock()
        dst_mock = MagicMock()
        prepare_client._src_connection = src_mock
        prepare_client._dst_connection = dst_mock
        prepare_client._src_user = "SRC USER"
        prepare_client._dst_user = "DST USER"

        prepare_client.disconnect()
        src_mock.close.assert_called_once()
        dst_mock.close.assert_called_once()

        self.assertIsNone(prepare_client._src_connection)
        self.assertIsNone(prepare_client._dst_connection)
        self.assertIsNone(prepare_client._src_user)
        self.assertIsNone(prepare_client._dst_user)

        # Should not raise any errors when already closed (such as when close() is called on a None object)
        prepare_client.disconnect()

    def test_connect_invalid_source_type(self, mock_cloner, mock_logger):
        self.mock_dataset['source']['type'] = 'nonexistent'
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)

        with self.assertRaises(NotImplementedError):
            prepare_client.connect()

    def test_clone(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        cloner_instance = mock_cloner.return_value
        cloner_instance.clone.return_value = 2840
        result = prepare_client.clone()
        self.assertEqual(2840, result)

        mock_cloner.assert_called_with(
            None,
            self.mock_dataset['source']['schema'],
            None,
            self.mock_dataset['destination']['schema'],
            self.mock_dataset['action'],
        )
        cloner_instance.clone.assert_called_once()


    def test_clone_invalid_source_type(self, mock_cloner, mock_logger):
        self.mock_dataset['source']['type'] = 'nonexistent'
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)

        with self.assertRaises(NotImplementedError):
            prepare_client.clone()

    def test_prepare(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.clone = MagicMock(return_value=3223)
        self.assertEqual({}, prepare_client.result)

        prepare_client.prepare()
        prepare_client.clone.assert_called_once()
        self.assertEqual({'rows_copied': 3223, 'action': 'clone'}, prepare_client.result)

    def test_prepare_invalid_action_type(self, mock_cloner, mock_logger):
        self.mock_dataset['action']['type'] = 'nonexistent'
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)

        with self.assertRaises(NotImplementedError):
            prepare_client.prepare()

    def test_get_result(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.result = {
            "rows_copied": 204,
            "action": "clone",
        }
        result = prepare_client.get_result()

        header_keys = [
            "someheader",
            "header",
            "process_id",
            "source_application",
            "destination_application",
            "version",
            "timestamp"
        ]
        summary_keys = [
            "rows_copied",
            "action",
        ]

        def keys_in_dict(keys: list, dct: dict):
            dict_keys = dct.keys()
            return len(dict_keys) == len(keys) and all([k in dct for k in keys])

        self.assertTrue(keys_in_dict(header_keys, result['header']))
        self.assertTrue(keys_in_dict(summary_keys, result['summary']))

    def test_start_prepare_process(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.connect = MagicMock()
        prepare_client.prepare = MagicMock()

        prepare_client.start_prepare_process()
        prepare_client.connect.assert_called_once()
        prepare_client.prepare.assert_called_once()

    def test_start_prepare_process_exception(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.connect = MagicMock(side_effect=Exception)

        prepare_client.start_prepare_process()
        mock_logger.error.assert_called_once()
