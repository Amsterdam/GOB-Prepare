from unittest import TestCase
from unittest.mock import MagicMock, patch, call

from gobprepare.prepare_client import PrepareClient
from tests import fixtures


@patch('gobprepare.prepare_client.logger')
class TestPrepareClient(TestCase):

    def setUp(self):
        self.mock_dataset = {
            'version': '0.1',
            'name': 'Test Dataset',
            'source': {
                'application': fixtures.random_string(),
                'type': 'oracle',
            },
            'destination': {
                'application': fixtures.random_string(),
                'type': 'postgres',
            },
            'actions': [{
                'source_schema': fixtures.random_string(),
                'destination_schema': fixtures.random_string(),
                'type': 'clone',
                'mask': {
                    "sometable": {
                        "somecolumn": "mask"
                    }
                }
            }]
        }

        self.mock_msg = {
            'header': {
                "someheader": "value",
            },
        }

    def test_init(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        # Expect a process_id is created
        self.assertTrue(prepare_client.process_id)
        self.assertEqual(self.mock_msg['header'], prepare_client.header)

        # Assert the logger is configured and called
        mock_logger.set_name.assert_called()
        mock_logger.set_default_args.assert_called()
        mock_logger.info.assert_called()

    def test_connect(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._connect_application = MagicMock(return_value=("connection", "user"))
        prepare_client.connect()

        prepare_client._connect_application.assert_has_calls([
            call(self.mock_dataset['source']),
            call(self.mock_dataset['destination']),
        ])
        self.assertEqual(prepare_client._src_connection, "connection")
        self.assertEqual(prepare_client._src_user, "user")
        self.assertEqual(prepare_client._dst_connection, "connection")
        self.assertEqual(prepare_client._dst_user, "user")

    @patch("gobprepare.prepare_client.connect_to_oracle", return_value=["connection", "user"])
    @patch("gobprepare.prepare_client.get_database_config", return_value={})
    def test_connect_application(self, mock_database_config, mock_connect_oracle, mock_logger):
        application_config = {
            "type": "oracle",
            "application": "SOME_APPLICATION",
        }
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        # Reset counter
        mock_logger.info.reset_mock()
        result = prepare_client._connect_application(application_config)
        self.assertEqual(("connection", "user"), result)
        mock_connect_oracle.assert_called_once()
        self.assertEqual(1, mock_logger.info.call_count)
        mock_database_config.assert_called_with("SOME_APPLICATION")
        mock_connect_oracle.assert_called_with(mock_database_config.return_value)

    @patch("gobprepare.prepare_client.connect_to_postgresql", return_value=["connection", "user"])
    @patch("gobprepare.prepare_client.get_database_config", return_value={})
    def test_connect_application_postgres(self, mock_database_config, mock_connect_postgres, mock_logger):
        application_config = {
            "type": "postgres",
            "application": "SOME_APPLICATION",
        }
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        # Reset counter
        mock_logger.info.reset_mock()
        result = prepare_client._connect_application(application_config)
        self.assertEqual(("connection", "user"), result)
        mock_connect_postgres.assert_called_once()
        self.assertEqual(1, mock_logger.info.call_count)
        mock_database_config.assert_called_with("SOME_APPLICATION")
        mock_connect_postgres.assert_called_with(mock_database_config.return_value)

    def test_connect_application_not_existing(self, mock_logger):
        application_config = {
            "type": "nonexisting",
            "application": "SOME_APPLICATION"
        }
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)

        with self.assertRaises(NotImplementedError):
            prepare_client._connect_application(application_config)

    def test_disconnect(self, mock_logger):
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

    def test_connect_invalid_source_type(self, mock_logger):
        self.mock_dataset['source']['type'] = 'nonexistent'
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)

        with self.assertRaises(NotImplementedError):
            prepare_client.connect()

    @patch("gobprepare.prepare_client.OracleToPostgresCloner", autospec=True)
    def test_action_clone(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        cloner_instance = mock_cloner.return_value
        cloner_instance.clone.return_value = 2840

        clone_action = self.mock_dataset['actions'][0]
        result = prepare_client.action_clone(clone_action)
        self.assertEqual(2840, result)

        mock_cloner.assert_called_with(
            None,
            clone_action['source_schema'],
            None,
            clone_action['destination_schema'],
            self.mock_dataset['actions'][0],
        )
        cloner_instance.clone.assert_called_once()

    def test_action_clone_invalid_source_destination_types(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        invalid_combinations = [
            ("oracle", "nonexistent"),
            ("nonexistent", "postgres"),
            ("nonexistent", "nonexistent")
        ]

        for source, destination in invalid_combinations:
            prepare_client.source['type'] = source
            prepare_client.destination['type'] = destination
            with self.assertRaises(NotImplementedError):
                prepare_client.action_clone({})

    @patch("gobprepare.prepare_client.drop_schema")
    @patch("gobprepare.prepare_client.create_schema")
    def test_action_clear(self, mock_create_schema, mock_drop_schema, mock_logger):
        action = {
            'type': 'postgres',
            'schemas': ['schema_a', 'schema_b'],
        }
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_clear(action)

        mock_drop_schema.assert_has_calls([
            call(prepare_client._dst_connection, 'schema_a'),
            call(prepare_client._dst_connection, 'schema_b'),
        ])
        mock_create_schema.assert_has_calls([
            call(prepare_client._dst_connection, 'schema_a'),
            call(prepare_client._dst_connection, 'schema_b'),
        ])

    def test_action_clear_nonexistent_type(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.destination['type'] = 'nonexistent'

        with self.assertRaises(NotImplementedError):
            prepare_client.action_clear({})

    @patch("gobprepare.prepare_client.OracleToPostgresSelector", autospec=True)
    def test_action_select(self, mock_selector, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        mock_selector_instance = mock_selector.return_value
        mock_selector_instance.select.return_value = 82840
        action = {
            "type": "select",
        }

        result = prepare_client.action_select(action)
        self.assertEqual(82840, result)
        mock_selector.assert_called_with(
            prepare_client._src_connection,
            prepare_client._dst_connection,
            action
        )
        mock_selector_instance.select.assert_called_once()

    def test_action_select_invalid_source_destination_types(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        invalid_combinations = [
            ("oracle", "nonexistent"),
            ("nonexistent", "postgres"),
            ("nonexistent", "nonexistent")
        ]

        for source, destination in invalid_combinations:
            prepare_client.source['type'] = source
            prepare_client.destination['type'] = destination
            with self.assertRaises(NotImplementedError):
                prepare_client.action_select({})


    def test_prepare(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._run_prepare_action = MagicMock(return_value="result")
        prepare_client._actions = ['action_a', 'action_b']
        prepare_client.prepare()

        self.assertEqual(['result', 'result'], prepare_client.result['actions'])
        prepare_client._run_prepare_action.assert_has_calls([
            call('action_a'),
            call('action_b')
        ])

    def test_run_prepare_action_clone(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_clone = MagicMock(return_value=3223)
        action = {
            "type": "clone"
        }

        result = prepare_client._run_prepare_action(action)
        prepare_client.action_clone.assert_called_with(action)
        self.assertEqual({
            "action": "clone",
            "rows_copied": 3223,
        }, result)

    def test_run_prepare_action_clear(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_clear = MagicMock()
        action = {
            "type": "clear"
        }

        result = prepare_client._run_prepare_action(action)
        prepare_client.action_clear.assert_called_with(action)
        self.assertEqual({
            "action": "clear",
            "clear": "OK",
        }, result)

    def test_run_prepare_action_select(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_select = MagicMock(return_value=3223)
        action = {
            "type": "select"
        }

        result = prepare_client._run_prepare_action(action)
        prepare_client.action_select.assert_called_with(action)
        self.assertEqual({
            "action": "select",
            "rows_copied": 3223,
        }, result)

    def test_prepare_invalid_action_type(self, mock_logger):
        self.mock_dataset['actions'][0]['type'] = 'nonexistent'
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)

        with self.assertRaises(NotImplementedError):
            prepare_client.prepare()

    def test_get_result(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.result = {
            "actions": [
                {
                    "rows_copied": 204,
                    "action": "clone",
                }
            ]
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
        self.assertTrue(keys_in_dict(summary_keys, result['summary']['actions'][0]))

    def test_start_prepare_process(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.connect = MagicMock()
        prepare_client.prepare = MagicMock()

        prepare_client.start_prepare_process()
        prepare_client.connect.assert_called_once()
        prepare_client.prepare.assert_called_once()

    def test_start_prepare_process_exception(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.connect = MagicMock(side_effect=Exception)

        prepare_client.start_prepare_process()
        mock_logger.error.assert_called_once()
