from unittest import TestCase
from unittest.mock import MagicMock, patch, call, mock_open, ANY

from gobcore.exceptions import GOBException
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
                'source': 'src',
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
                "catalogue": "somecatalogue",
            },
        }

    def test_init(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        # Expect a process_id is created
        self.assertTrue(prepare_client.process_id)
        self.assertEqual(self.mock_msg['header'], prepare_client.header)

        # Assert the logger is configured and called
        mock_logger.configure.assert_called_with({'header': ANY}, "PREPARE")

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
        prepare_client._close_connection = MagicMock()
        src_mock = MagicMock()
        dst_mock = MagicMock()
        prepare_client._src_connection = src_mock
        prepare_client._dst_connection = dst_mock
        prepare_client._src_user = "SRC USER"
        prepare_client._dst_user = "DST USER"

        prepare_client.disconnect()

        self.assertIsNone(prepare_client._src_connection)
        self.assertIsNone(prepare_client._dst_connection)
        self.assertIsNone(prepare_client._src_user)
        self.assertIsNone(prepare_client._dst_user)

        prepare_client._close_connection.assert_has_calls([
            call(src_mock),
            call(dst_mock),
        ])

        # Should not raise any errors when already closed (such as when close() is called on a None object)
        prepare_client.disconnect()

    def test_close_connection(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        connection = MagicMock()
        prepare_client._close_connection(connection)
        connection.close.assert_called_once()

    def test_close_connection_exception(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        connection = MagicMock()
        connection.close.side_effect = Exception
        prepare_client._close_connection(connection)

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

    @patch("gobprepare.prepare_client.drop_table")
    @patch("gobprepare.prepare_client.create_schema")
    @patch("gobprepare.prepare_client.list_tables_for_schema")
    def test_action_clear(self, mock_list_tables, mock_create_schema, mock_drop_table, mock_logger):
        mock_list_tables.return_value = ['table_a', 'table_b']
        action = {
            'type': 'postgres',
            'schemas': ['schema_a', 'schema_b'],
        }
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_clear(action)

        mock_list_tables.assert_has_calls([
            call(prepare_client._dst_connection, 'schema_a'),
            call(prepare_client._dst_connection, 'schema_b'),
        ])
        mock_drop_table.assert_has_calls([
            call(prepare_client._dst_connection, 'schema_a.table_a'),
            call(prepare_client._dst_connection, 'schema_a.table_b'),
            call(prepare_client._dst_connection, 'schema_b.table_a'),
            call(prepare_client._dst_connection, 'schema_b.table_b'),
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
    def test_action_select_src(self, mock_selector, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        mock_selector_instance = mock_selector.return_value
        mock_selector_instance.select.return_value = 82840
        action = {
            "type": "select",
            "source": "src",
        }

        result = prepare_client.action_select(action)
        self.assertEqual(82840, result)
        mock_selector.assert_called_with(
            prepare_client._src_connection,
            prepare_client._dst_connection,
            action
        )
        mock_selector_instance.select.assert_called_once()

    @patch("gobprepare.prepare_client.PostgresToPostgresSelector", autospec=True)
    def test_action_select_dst(self, mock_selector, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        mock_selector_instance = mock_selector.return_value
        mock_selector_instance.select.return_value = 82840
        action = {
            "type": "select",
            "source": "dst",
        }

        result = prepare_client.action_select(action)
        self.assertEqual(82840, result)
        # Should be called with dst_connection as both source and destination for selector
        mock_selector.assert_called_with(
            prepare_client._dst_connection,
            prepare_client._dst_connection,
            action
        )
        mock_selector_instance.select.assert_called_once()

    def test_action_select_invalid_source_destination_types(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        invalid_combinations = [
            ("src", "oracle", "nonexistent"),
            ("src", "nonexistent", "postgres"),
            ("src", "nonexistent", "nonexistent"),
            ("dst", "oracle", "nonexistent"),
            ("dst", "nonexistent", "nonexistent"),
            ("nonexistent", "oracle", "postgres"),
        ]

        for source_type, source, destination in invalid_combinations:
            prepare_client.source['type'] = source
            prepare_client.destination['type'] = destination
            with self.assertRaises(NotImplementedError):
                prepare_client.action_select({'source': source_type})

    @patch("gobprepare.prepare_client.execute_postgresql_query")
    def test_action_execute_sql(self, mock_execute, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._get_query = MagicMock(return_value="the_query")

        action = {"action": "execute_sql", "query": "SOME QUERY", "description": "Execute the query"}
        prepare_client.action_execute_sql(action)
        prepare_client._get_query.assert_called_with(action)
        mock_execute.assert_called_with(prepare_client._dst_connection, "the_query")

    def test_action_execute_sql_invalid_dst(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.destination["type"] = "somedb"

        with self.assertRaises(NotImplementedError):
            prepare_client.action_execute_sql({})

    @patch("gobprepare.prepare_client.PostgresCsvImporter", autospec=True)
    def test_action_import_csv(self, mock_importer, mock_logger):
        action = {
            "action": "import_csv",
            "destination": "some_table",
            "somemore": "configuration",
        }
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.destination["type"] = "postgres"
        mock_importer_instance = mock_importer.return_value
        mock_importer_instance.import_csv.return_value = 4802

        result = prepare_client.action_import_csv(action)
        self.assertEqual(4802, result)

        mock_importer.assert_called_with(prepare_client._dst_connection, action)
        mock_importer_instance.import_csv.assert_called_once()

    def test_action_import_csv_invalid_destination(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.destination["type"] = "somedb"

        with self.assertRaises(NotImplementedError):
            prepare_client.action_import_csv({})

    def test_action_join_actions(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        action = {
            "type": "join_actions",
            "id": "id",
        }

        result = prepare_client._run_prepare_action(action)

    def test_run_prepare_action_invalid_action_type(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        action = {
            "type": "nonexistent",
            "id": "id",
        }
        with self.assertRaises(NotImplementedError):
            result = prepare_client._run_prepare_action(action)

    def test__get_query_string_type(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        action = {
            "query_src": "string",
            "query": "SELECT SOMETHING FROM SOMEWHERE WHERE SOMETHING IS TRUE",
        }

        result = prepare_client._get_query(action)
        self.assertEqual(action["query"], result)

    def test__get_query_string_as_list_type(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        config = {
            "query_src": "string",
            "query": ["SELECT", "SOMETHING", "FROM", "SOMEWHERE"],
        }
        result = prepare_client._get_query(config)
        self.assertEqual("\n".join(config['query']), result)

    @patch("builtins.open", new_callable=mock_open, read_data="the query")
    def test__get_query_file_type(self, mock_file_open, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        action = {
            "query_src": "file",
            "query": "some/file/path.sql",
        }
        result = prepare_client._get_query(action)
        mock_file_open.assert_called_with(action['query'])
        self.assertEqual("the query", result)

    def test__get_query_invalid_type(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        action = {
            "query_src": "invalid"
        }
        with self.assertRaises(GOBException):
            prepare_client._get_query(action)

    def test_run_prepare_action_clone(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_clone = MagicMock(return_value=3223)
        action = {
            "type": "clone",
            "id": "id",
        }

        result = prepare_client._run_prepare_action(action)
        prepare_client.action_clone.assert_called_with(action)
        self.assertEqual({
            "action": "clone",
            "rows_copied": 3223,
            "id": "id",
        }, result)

    def test_run_prepare_action_clear(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_clear = MagicMock()
        action = {
            "type": "clear",
            "id": "id",
        }

        result = prepare_client._run_prepare_action(action)
        prepare_client.action_clear.assert_called_with(action)
        self.assertEqual({
            "action": "clear",
            "clear": "OK",
            "id": "id",
        }, result)

    def test_run_prepare_action_select(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_select = MagicMock(return_value=3223)
        action = {
            "type": "select",
            "id": "id",
        }

        result = prepare_client._run_prepare_action(action)
        prepare_client.action_select.assert_called_with(action)
        self.assertEqual({
            "action": "select",
            "rows_copied": 3223,
            "id": "id",
        }, result)

    def test_run_prepare_action_execute_sql(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_execute_sql = MagicMock()
        action = {
            "type": "execute_sql",
            "id": "id",
        }

        result = prepare_client._run_prepare_action(action)
        prepare_client.action_execute_sql.assert_called_with(action)
        self.assertEqual({
            "action": "execute_sql",
            "executed": "OK",
            "id": "id",
        }, result)

    def test_run_prepare_action_import_csv(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_import_csv = MagicMock(return_value=425)
        action = {
            "type": "import_csv",
            "id": "id",
        }

        result = prepare_client._run_prepare_action(action)
        prepare_client.action_import_csv.assert_called_with(action)
        self.assertEqual({
            "action": "import_csv",
            "rows_imported": 425,
            "id": "id",
        }, result)

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
        result = prepare_client._get_result()

        header_keys = [
            "someheader",
            "header",
            "process_id",
            "source",
            'application',
            "source_application",
            "destination_application",
            "version",
            "timestamp",
            "catalogue",
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
        mock_logger.reset_calls()
        prepare_client._create_tasks = MagicMock()
        prepare_client._get_task_message = MagicMock()

        res = prepare_client.start_prepare_process()
        mock_logger.info.assert_called_once()
        mock_logger.warning.assert_not_called()
        prepare_client._get_task_message.assert_called_with(prepare_client._create_tasks.return_value)
        self.assertEqual(prepare_client._get_task_message.return_value, res)

    def test_create_tasks(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._actions = [{
            'id': 'action1',
            'depends_on': ['dep1', 'dep2'],
            'type': 'some type',
        }, {
            'id': 'action2',
            'type': 'some other type',
        }]

        self.assertEqual([{
            'id': 'action1',
            'dependencies': ['dep1', 'dep2']
        }, {
            'id': 'action2',
            'dependencies': []
        }], prepare_client._create_tasks())

    def test_create_tasks_clone(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._split_clone_action = MagicMock(return_value=['task_a', 'task_b'])
        prepare_client._actions = [{
            'id': 'action',
            'type': 'clone'
        }]
        result = prepare_client._create_tasks()
        self.assertEqual(prepare_client._split_clone_action.return_value, result)

    def test_get_task_message(self, mock_logger):
        tasks = [{'id': 'task1', 'dependencies': []}, {'id': 'task2', 'dependencies': ['task1']}]
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.header = {'hea': 'der', 'catalogue': 'somecatalogue'}
        prepare_client.msg = {'prepare_config': 'config.json'}
        res = prepare_client._get_task_message(tasks)

        self.assertEqual({
            'header': {
                'hea': 'der',
                'catalogue': 'somecatalogue',
                'extra': {
                    'catalogue': 'somecatalogue'
                },
            },
            'contents': {
                'tasks': tasks,
                'key_prefix': 'prepare'
            }
        }, res)

    def test_run_prepare_task(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.connect = MagicMock()
        prepare_client._run_prepare_action = MagicMock()
        prepare_client.disconnect = MagicMock()
        prepare_client._get_result = MagicMock()

        prepare_client._actions = [{'id': 'some_id'}, {'id': 'other_id'}]
        prepare_client.msg = {'id': 'other_id'}

        result = prepare_client.run_prepare_task()
        prepare_client._run_prepare_action.assert_called_with(prepare_client._actions[1])

        self.assertEqual(result, prepare_client._get_result.return_value)

    def test_run_prepare_task_no_action(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._actions = [{'id': 'some_id'}, {'id': 'other_id'}]
        prepare_client.msg = {'id': 'nonexistent'}

        with self.assertRaises(GOBException):
            prepare_client.run_prepare_task()

    def test_run_prepare_task_original_action_and_override(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.connect = MagicMock()
        prepare_client._run_prepare_action = MagicMock()
        prepare_client.disconnect = MagicMock()
        prepare_client._get_result = MagicMock()
        prepare_client._actions = [{'id': 'some_id'}, {'id': 'other_id', 'type': 'sometype'}]
        prepare_client.msg = {'id': 'some_other_id', 'original_action': 'other_id', 'override': {'type': 'newtype'}}

        prepare_client.run_prepare_task()

        # Take original action based on original_action and override 'type'
        prepare_client._run_prepare_action.assert_called_with({
            'id': 'other_id',
            'type': 'newtype'
        })

    def test_complete_prepare_process(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.msg['summary'] = {'key': 'value'}

        result = prepare_client.complete_prepare_process()
        self.assertEquals([], result['contents'])

    def test_split_clone_action(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.connect = MagicMock()
        prepare_client.disconnect = MagicMock()
        prepare_client._get_cloner = MagicMock()

        prepare_client._get_cloner.return_value.read_source_table_names.return_value = ['table_a', 'table_b']

        action = {
            'type': 'clone',
            'id': 'some_clone_action',
            'depends_on': ['some_dep']
        }

        result = prepare_client._split_clone_action(action)

        expected_result = [{
            'id': 'some_clone_action__table_a',
            'dependencies': action['depends_on'],
            'extra_msg': {
                'override': {
                    'include': ['table_a'],
                    'ignore': []
                },
                'original_action': 'some_clone_action',
            }
        }, {
            'id': 'some_clone_action__table_b',
            'dependencies': action['depends_on'],
            'extra_msg': {
                'override': {
                    'include': ['table_b'],
                    'ignore': []
                },
                'original_action': 'some_clone_action',
            }
        }, {
            'id': action['id'],
            'dependencies': ['some_clone_action__table_a', 'some_clone_action__table_b'],
            'extra_msg': {
                'override': {
                    'type': 'join_actions'
                }
            }
        }]
        self.assertEqual(expected_result, result)
