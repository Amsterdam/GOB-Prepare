from unittest import TestCase
from unittest.mock import MagicMock, call, mock_open, patch

from gobcore.exceptions import GOBException

from gobprepare.prepare_client import OracleDatastore, PrepareClient, SqlDatastore
from gobprepare.utils.exceptions import DuplicateTableError
from tests import fixtures

mock_oracle = MagicMock(spec=OracleDatastore)
mock_sql = MagicMock(spec=SqlDatastore)

mock_factory = MagicMock()
mock_factory.get_datastore.side_effect = [
    mock_oracle,
    mock_sql,
]


@patch('gobprepare.prepare_client.logger')
@patch('gobprepare.prepare_client.get_datastore_config', MagicMock(side_effect=lambda x: x))
@patch('gobprepare.prepare_client.DatastoreFactory', mock_factory)
class TestPrepareClientInit(TestCase):

    def setUp(self):
        self.mock_dataset = {
            'version': '0.1',
            'name': 'Test Dataset',
            'source': {
                'application': fixtures.random_string()
            },
            'destination': {
                'application': fixtures.random_string()
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
            }],
            'publish_schemas': {
                'src schema': 'dst schema',
            }
        }

        self.mock_msg = {
            'header': {
                "someheader": "value",
                "catalogue": "somecatalogue",
            },
        }

    def test_init(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        self.assertEqual(self.mock_msg['header'], prepare_client.header)
        self.assertEqual({'src schema': 'dst schema'}, prepare_client.publish_schemas)

        mock_factory.get_datastore.assert_has_calls([
            call(self.mock_dataset['source']['application'], {}),
            call(self.mock_dataset['destination']['application'], {}),
        ])
        self.assertEqual(mock_oracle, prepare_client._src_datastore)
        self.assertEqual(mock_sql, prepare_client._dst_datastore)


@patch('gobprepare.prepare_client.logger')
@patch('gobprepare.prepare_client.PrepareClient._set_datastores', MagicMock())
@patch('gobprepare.prepare_client.PrepareClient._src_datastore', MagicMock())
@patch('gobprepare.prepare_client.PrepareClient._dst_datastore', MagicMock())
class TestPrepareClient(TestCase):

    def setUp(self):
        self.mock_dataset = {
            'version': '0.1',
            'name': 'Test Dataset',
            'source': {
                'application': fixtures.random_string()
            },
            'destination': {
                'application': fixtures.random_string()
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
            }],
            'publish_schemas': {
                'src schema': 'dst schema',
            }
        }

        self.mock_msg = {
            'header': {
                "someheader": "value",
                "catalogue": "somecatalogue",
            },
        }


    def test_connect(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.connect()

        prepare_client._src_datastore.connect.assert_called_once()
        prepare_client._dst_datastore.connect.assert_called_once()

    def test_disconnect(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        src_datastore = prepare_client._src_datastore
        dst_datastore = prepare_client._dst_datastore
        prepare_client.disconnect()
        src_datastore.disconnect.assert_called_once()
        dst_datastore.disconnect.assert_called_once()

        self.assertIsNone(prepare_client._src_datastore)
        self.assertIsNone(prepare_client._dst_datastore)

    @patch("gobprepare.prepare_client.OracleToPostgresCloner", autospec=True)
    def test_action_clone(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        cloner_instance = mock_cloner.return_value
        cloner_instance.clone.return_value = 2840

        clone_action = self.mock_dataset['actions'][0]
        result = prepare_client.action_clone(clone_action)
        self.assertEqual(2840, result)

        mock_cloner.assert_called_with(
            prepare_client._src_datastore,
            clone_action['source_schema'],
            prepare_client._dst_datastore,
            clone_action['destination_schema'],
            self.mock_dataset['actions'][0],
        )
        cloner_instance.clone.assert_called_once()

    def test_action_clear(self, mock_logger):
        action = {
            'type': 'postgres',
            'schemas': ['schema_a', 'schema_b'],
        }
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._dst_datastore.list_tables_for_schema.return_value = ['table_a', 'table_b']
        prepare_client.action_clear(action)

        prepare_client._dst_datastore.create_schema.assert_has_calls([
            call('schema_a'),
            call('schema_b'),
        ])
        prepare_client._dst_datastore.drop_table.assert_has_calls([
            call('schema_a.table_a'),
            call('schema_a.table_b'),
            call('schema_b.table_a'),
            call('schema_b.table_b'),
        ])
        prepare_client._dst_datastore.list_tables_for_schema.assert_has_calls([
            call('schema_a'),
            call('schema_b'),
        ])

    @patch("gobprepare.prepare_client.DatastoreToPostgresSelector", autospec=True)
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
            prepare_client._src_datastore,
            prepare_client._dst_datastore,
            action
        )
        mock_selector_instance.select.assert_called_once()

    @patch("gobprepare.prepare_client.DatastoreToPostgresSelector", autospec=True)
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
            prepare_client._dst_datastore,
            prepare_client._dst_datastore,
            action
        )
        mock_selector_instance.select.assert_called_once()

    def test_action_execute_sql(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._get_query = MagicMock(return_value="the_query")

        action = {"action": "execute_sql", "query": "SOME QUERY", "description": "Execute the query"}
        prepare_client.action_execute_sql(action)
        prepare_client._get_query.assert_called_with(action)
        prepare_client._dst_datastore.execute.assert_called_with("the_query")

    @patch("gobprepare.prepare_client.SqlCsvImporter", autospec=True)
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

        mock_importer.assert_called_with(prepare_client._dst_datastore, action)
        mock_importer_instance.import_csv.assert_called_once()

    @patch("gobprepare.prepare_client.create_table_columnar_as_query")
    def test_action_create_table(self, mock_create_table, mock_logger):
        action = {
            "id": "create_this_table",
            "type": "create_table",
            "table_name": "schema.table_name",
            "query": ["select * from laladiela"],
            "query_src": "string"
        }
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._dst_datastore = MagicMock()
        prepare_client._run_prepare_action(action)
        mock_create_table.assert_called_with(
            prepare_client._dst_datastore,
            "schema.table_name",
            "select * from laladiela"
        )
        prepare_client._dst_datastore.execute.assert_has_calls([
            call(mock_create_table.return_value),
            call("ANALYZE schema.table_name")
        ])
        mock_logger.info.assert_called_with("Created table 'schema.table_name'")

    @patch("gobprepare.prepare_client.SqlAPIImporter", autospec=True)
    def test_action_import_api(self, mock_importer, mock_logger):
        action = {
            "action": "import_api",
            "destination": "some_table",
            "somemore": "configuration",
        }
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._dst_connection = "connection"
        prepare_client._get_query = MagicMock(return_value="the_query")
        prepare_client.destination["type"] = "postgres"
        mock_importer_instance = mock_importer.return_value
        mock_importer_instance.import_api.return_value = 77

        result = prepare_client.action_import_api(action)
        self.assertEqual(77, result)

        mock_importer.assert_called_with(prepare_client._dst_datastore, action, "the_query")
        mock_importer_instance.import_api.assert_called_once()

    @patch("gobprepare.prepare_client.SqlDumpImporter", autospec=True)
    def test_action_import_sql_dump(self, mock_importer, mock_logger):
        action = {
            "action": "import_dump",
            "destination": "some_table",
            "somemore": "configuration",
        }
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.destination["type"] = "postgres"
        mock_importer_instance = mock_importer.return_value
        mock_importer_instance.import_dump.return_value = "somefile.sql.gz"

        result = prepare_client.action_import_sql_dump(action)
        self.assertEqual( "somefile.sql.gz", result)

        mock_importer.assert_called_with(prepare_client._dst_datastore, action)
        mock_importer_instance.import_dump.assert_called_once()

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

    def test_run_prepare_action_import_api(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_import_api = MagicMock(return_value=88)
        action = {
            "type": "import_api",
            "id": "id",
        }

        result = prepare_client._run_prepare_action(action)
        prepare_client.action_import_api.assert_called_with(action)
        self.assertEqual({
            "action": "import_api",
            "rows_imported": 88,
            "id": "id",
        }, result)

    def test_run_prepare_action_import_sql_dump(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.action_import_sql_dump = MagicMock(return_value="somefile.sql.gz")
        action = {
            "type": "import_dump",
            "id": "id",
        }

        result = prepare_client._run_prepare_action(action)
        prepare_client.action_import_sql_dump.assert_called_with(action)
        self.assertEqual({
            "action": "import_dump",
            "file_imported": "somefile.sql.gz",
            "id": "id",
        }, result)

    def test_run_prepare_action_publish_schemas(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._publish_schema = MagicMock()
        action = {
            'type': 'publish_schemas',
            'id': 'id',
            'publish_schemas': {
                'src a': 'dst a',
                'src b': 'dst b',
            }
        }
        result = prepare_client._run_prepare_action(action)
        self.assertEqual({
            'id': 'id',
            'action': 'publish_schemas',
            'published_schemas': ['dst a', 'dst b']
        }, result)
        prepare_client._publish_schema.assert_has_calls([
            call('src a', 'dst a'),
            call('src b', 'dst b'),
        ])

    def test_run_prepare_action_check_row_counts(self, mock_logger):
        """Test PrepareClient.action_check_row_counts."""
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        action = {
            "type": "check_row_counts",
            'id': 'action_id',
            "table_row_counts": {"ok_table": 100, "growing_table": 500},
            "margin_percentage": 5
        }

        prepare_client._dst_datastore.table_count = MagicMock(side_effect=[104, 509])
        result = prepare_client._run_prepare_action(action)
        prepare_client._dst_datastore.table_count.assert_has_calls([
            call("ok_table"), call("growing_table")
        ])
        self.assertEqual(result["action"], "check_row_counts")
        self.assertEqual(result["id"], "action_id")
        self.assertTrue(result["row_count_check"])

        prepare_client._dst_datastore.table_count.side_effect=[96, 400]
        result = prepare_client._run_prepare_action(action)
        prepare_client._dst_datastore.table_count.assert_has_calls([
            call("ok_table"), call("growing_table")
        ])
        self.assertFalse(result["row_count_check"])
        mock_logger.error.assert_called_with("Deviation of -20.00% for growing_table! Expected 500 rows, got 400")

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
        }, {
            'id': 'final_action',
            'type': 'some type',
            'depends_on': '*'
        }]

        self.assertEqual([{
            'task_name': 'action1',
            'dependencies': ['dep1', 'dep2']
        }, {
            'task_name': 'action2',
            'dependencies': []
        }, {
            'task_name': 'final_action',
            'dependencies': ['action1', 'action2']
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
        tasks = [{'task_name': 'task1', 'dependencies': []}, {'task_name': 'task2', 'dependencies': ['task1']}]
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
        prepare_client.header = {'task_name': 'other_id'}

        result = prepare_client.run_prepare_task()
        prepare_client._run_prepare_action.assert_called_with(prepare_client._actions[1])

        self.assertEqual(result, prepare_client._get_result.return_value)

    def test_run_prepare_task_no_action(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._actions = [{'id': 'some_id'}, {'id': 'other_id'}]
        prepare_client.header = {'task_name': 'nonexistent'}

        with self.assertRaises(GOBException):
            prepare_client.run_prepare_task()

    def test_run_prepare_task_duplicate_table(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._actions = [{'id': 'some_id'}, {'id': 'other_id'}]
        prepare_client.header = {'task_name': 'some_id'}
        prepare_client._run_prepare_action = MagicMock()
        prepare_client._run_prepare_action.side_effect = DuplicateTableError
        result = prepare_client.run_prepare_task()
        self.assertFalse(result)

    def test_run_prepare_task_original_action_and_override(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.connect = MagicMock()
        prepare_client._run_prepare_action = MagicMock()
        prepare_client.disconnect = MagicMock()
        prepare_client._get_result = MagicMock()
        prepare_client._actions = [{'id': 'some_id'}, {'id': 'other_id', 'type': 'sometype'}]
        prepare_client.header = {'task_name': 'some_other_id'}
        prepare_client.msg = {'original_action': 'other_id', 'override': {'type': 'newtype'}}

        prepare_client.run_prepare_task()

        # Take original action based on original_action and override 'type'
        prepare_client._run_prepare_action.assert_called_with({
            'id': 'other_id',
            'type': 'newtype'
        })

    def test_publish_schema(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client._dst_connection = MagicMock()

        prepare_client._publish_schema('src schema', 'dst schema')

        prepare_client._dst_datastore.drop_schema.assert_called_with('dst schema')
        prepare_client._dst_datastore.rename_schema.assert_called_with('src schema', 'dst schema')

        with self.assertRaises(GOBException):
            prepare_client._publish_schema('same src and dst', 'same src and dst')

    def test_complete_prepare_process(self, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset, self.mock_msg)
        prepare_client.msg['summary'] = {'key': 'value'}

        result = prepare_client.complete_prepare_process()
        self.assertEqual([], result['contents'])

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
            'task_name': 'some_clone_action__table_a',
            'dependencies': action['depends_on'],
            'extra_msg': {
                'override': {
                    'include': ['^table_a$'],
                    'ignore': []
                },
                'original_action': 'some_clone_action',
            }
        }, {
            'task_name': 'some_clone_action__table_b',
            'dependencies': action['depends_on'],
            'extra_msg': {
                'override': {
                    'include': ['^table_b$'],
                    'ignore': []
                },
                'original_action': 'some_clone_action',
            }
        }, {
            'task_name': action['id'],
            'dependencies': ['some_clone_action__table_a', 'some_clone_action__table_b'],
            'extra_msg': {
                'override': {
                    'type': 'join_actions'
                }
            }
        }]
        self.assertEqual(expected_result, result)
