"""
Contains the PrepareClient class.

The PrepareClient reads a dataset definition with a source and destination database and the actions
to perform on the data. For now the only supported action is a data clone from an Oracle to a Postgres
database.
"""

import datetime

from gobcore.datastore.factory import DatastoreFactory
from gobcore.datastore.oracle import OracleDatastore
from gobcore.datastore.sql import SqlDatastore
from gobconfig.datastore.config import get_datastore_config
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import PREPARE
from gobprepare.cloner.oracle_to_sql import OracleToSqlCloner
from gobprepare.importers.api_importer import SqlAPIImporter
from gobprepare.importers.csv_importer import SqlCsvImporter
from gobprepare.selector.datastore_to_postgres import DatastoreToPostgresSelector
from gobprepare.utils.exceptions import DuplicateTableError

READ_BATCH_SIZE = 100000
WRITE_BATCH_SIZE = 100000


class PrepareClient:
    """
    PrepareClient

    Construct using a prepare definition and call start_prepare_client() to start the data preparation.

    Handles everything from reading prepare definition to calling the appropriate helpers.
    """
    result = {}
    prepares_imports = None
    _src_datastore = None
    _dst_datastore = None

    def __init__(self, prepare_config, msg):
        self.header = msg.get('header', {})
        self.msg = msg
        self._prepare_config = prepare_config
        self._actions = prepare_config['actions']
        self._name = prepare_config['name']
        self.source = self._prepare_config.get('source')
        self.source_app = self._prepare_config.get('source', {}).get('application')
        self.destination = self._prepare_config['destination']
        self.destination_app = self._prepare_config['destination']['application']
        self.publish_schemas = self._prepare_config.get('publish_schemas', {})

        assert isinstance(self.publish_schemas, dict)

        self.header.update({
            'source': self.source_app,
            'application': self.source_app,
        })
        msg["header"] = self.header

        logger.configure(msg, "PREPARE")

        self._set_datastores()

    def _set_datastores(self):
        if self.source:
            self._src_datastore = DatastoreFactory.get_datastore(
                get_datastore_config(self.source['application']),
                self.source.get('read_config', {})
            )

            # This class only supports OracleDatastore as source
            assert isinstance(self._src_datastore, OracleDatastore), \
                "Only Oracle Datastore is currently supported as src"

        self._dst_datastore = DatastoreFactory.get_datastore(
            get_datastore_config(self.destination['application']),
            self.destination.get('read_config', {})
        )

        # This class assumes SqlDatastore as destination.
        assert isinstance(self._dst_datastore, SqlDatastore), "Only Sql Datastores are currently supported as dst"

    def connect(self):
        """Connects to data source and destination database

        :return:
        """
        if self._src_datastore:
            self._src_datastore.connect()
        self._dst_datastore.connect()

    def disconnect(self):
        """Closes open database connections

        :return:
        """
        if self._src_datastore:
            self._src_datastore.disconnect()
        self._dst_datastore.disconnect()
        self._src_datastore = None
        self._dst_datastore = None

    def _get_cloner(self, action):
        # Assert that src datastore is set. Dst datastore is always present (would have failed at initialisation otw)
        assert self._src_datastore is not None, "No src datastore set"

        return OracleToSqlCloner(
            self._src_datastore,
            action['source_schema'],
            self._dst_datastore,
            action['destination_schema'],
            action
        )

    def action_clone(self, action: dict) -> int:
        """Clones the source data using an external Cloner class.

        :return:
        """

        cloner = self._get_cloner(action)
        return cloner.clone()

    def action_clear(self, action: dict):
        """Clears destination database schema's (removes and recreates schemas)

        :param action:
        :return:
        """
        for schema in action['schemas']:
            logger.info(f"Clear schema {schema}")
            self._dst_datastore.create_schema(schema)
            tables = self._dst_datastore.list_tables_for_schema(schema)

            for table in tables:
                full_tablename = f"{schema}.{table}"
                self._dst_datastore.drop_table(full_tablename)

    def action_select(self, action: dict) -> int:
        """Select action. Selects data using query and inserts the data into destination database.

        :param action:
        :return:
        """
        if action['source'] == 'src':
            assert self._src_datastore is not None, "src datastore not set"

        selector = DatastoreToPostgresSelector(
            # Select from src_datastore or dst_datastore
            self._src_datastore if action['source'] == 'src' else self._dst_datastore,
            self._dst_datastore,
            action
        )
        rows = selector.select()
        return rows

    def action_execute_sql(self, action: dict):
        """Execute SQL action. Executes SQL on destination database.

        :param action:
        :return:
        """
        self._dst_datastore.execute(self._get_query(action))
        logger.info(f"Executed query '{action['description']}' on destination")

    def action_import_csv(self, action: dict):
        """Import CSV action. Import CSV into destination database

        :param action:
        :return:
        """
        importer = SqlCsvImporter(self._dst_datastore, action)

        rows_imported = importer.import_csv()
        logger.info(f"Imported {rows_imported} rows from CSV to table {action['destination']}")
        return rows_imported

    def action_import_api(self, action: dict):
        """Import API action. Import API into destination database action.

        :param action:
        :return:
        """
        query = self._get_query(action)
        importer = SqlAPIImporter(self._dst_datastore, action, query)

        rows_imported = importer.import_api()
        logger.info(f"Imported {rows_imported} rows from API to table {action['destination']}")
        return rows_imported

    def _get_query(self, action: dict):
        """Extracts query form action. Reads query from action or from file.

        :param action:
        :return:
        """
        src = action.get('query_src')

        if src == "string":
            # Multiline queries are represented as lists in JSON. Join list as string
            if isinstance(action["query"], list):
                return "\n".join(action["query"])
            return action["query"]
        elif src == "file":
            with open(action["query"]) as f:
                return f.read()

        raise GOBException("Missing or invalid 'query_src'")

    def _run_prepare_action(self, action: dict):  # noqa: C901
        """Calls appropriate action.

        :param action:
        :return:
        """
        result = {
            'id': action['id'],
            'action': action['type'],
        }
        if action["type"] == "clone":
            result["rows_copied"] = self.action_clone(action)
        elif action["type"] == "clear":
            self.action_clear(action)
            result["clear"] = "OK"
        elif action["type"] == "select":
            result["rows_copied"] = self.action_select(action)
        elif action["type"] == "execute_sql":
            self.action_execute_sql(action)
            result["executed"] = "OK"
        elif action["type"] == "import_csv":
            result["rows_imported"] = self.action_import_csv(action)
        elif action["type"] == "import_api":
            result["rows_imported"] = self.action_import_api(action)
        elif action["type"] == "join_actions":
            # Action only joins dependencies. No further actions necessary
            return None
        else:
            raise NotImplementedError

        self.result['action'] = result
        return result

    def _get_result(self):
        metadata = {
            **self.header,
            **self.msg,  # Return original message in header
            "source_application": self.source_app,
            "destination_application": self.destination_app,
            "version": self._prepare_config['version'],
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

        result = {
            "header": metadata,
            "summary": {
                **self.result,
                **logger.get_summary(),
            },
        }
        return result

    def _split_clone_action(self, action):
        """Splits a clone action into smaller tasks. Creates a task per table.

        :param action:
        :return:
        """
        self.connect()
        cloner = self._get_cloner(action)
        table_names = cloner.read_source_table_names()
        self.disconnect()

        tasks = []
        for table_name in table_names:
            tasks.append({
                'id': action['id'] + '__' + table_name.lower(),
                'dependencies': action.get('depends_on', []),
                'extra_msg': {
                    'override': {
                        # Include only this table. Unset the ignore list
                        'include': [f'^{table_name}$'],
                        'ignore': [],
                    },
                    'original_action': action['id'],
                }
            })

        ids = [task['id'] for task in tasks]

        # Create join action with dependencies on new steps
        tasks.append({
            'id': action['id'],
            'dependencies': ids,
            'extra_msg': {
                'override': {
                    'type': 'join_actions'
                }
            }
        })

        return tasks

    def _create_tasks(self):
        """Creates tasks to be put on the message bus from the actions defined in the config

        :return:
        """
        tasks = []
        for action in self._actions:
            if action['type'] == 'clone':
                # Split a clone action into smaller tasks
                tasks.extend(self._split_clone_action(action))
            else:
                tasks.append({
                    'id': action['id'],
                    'dependencies': action.get('depends_on', []),
                })
        return tasks

    def _get_task_message(self, tasks):
        """Publishes tasks for further processing.

        :param tasks:
        :return:
        """
        msg = {
            "header": {
                **self.header,
                "extra": {
                    "catalogue": self.header['catalogue'],
                }
            },
            "contents": {
                "tasks": tasks,
                "key_prefix": PREPARE,
            }
        }
        return msg

    def start_prepare_process(self):
        """Entry method. Starts the prepare process.

        :return:
        """
        app_msg = f" from {self.source_app}" if self.source_app else ""
        logger.info(f"Prepare dataset {self._name}{app_msg} started")

        tasks = self._create_tasks()
        return self._get_task_message(tasks)

    def run_prepare_task(self):
        """Runs incoming task.

        Checks if id is known in configuration. If not, we may have generated this task. In that case original_action
        will be set, possibly with override set.
        In that case, we copy the original action and use the override dict to update the action.

        :return:
        """
        taskid = self.msg['id']
        action = [action for action in self._actions if action['id'] == taskid]

        if not action and 'original_action' in self.msg:
            action = [action for action in self._actions if action['id'] == self.msg['original_action']]

        if not action:
            raise GOBException(f"Unknown action with id {taskid}")

        action = action[0]

        if 'override' in self.msg:
            action.update(self.msg['override'])

        logger.info(f"Running prepare task: '{taskid}' (action={action.get('type')})")
        self.connect()

        try:
            self._run_prepare_action(action)
        except DuplicateTableError as err:
            # delete table in dst (if created) and requeue the task message
            self._handle_duplicate_table_error()
            logger.warning(f'{err}, requeue task: \'{taskid}\'')
            return False
        else:
            return self._get_result()
        finally:
            self.disconnect()

    def _handle_duplicate_table_error(self):
        if self._prepare_config.destination_table.get('create', False):
            table = self._prepare_config.destination_table['name']
            self._dst_datastore.drop_table(table)
            logger.info(f'Table dropped: f{table}')

    def _publish_result_schemas(self):
        for src_schema, dst_schema in self.publish_schemas.items():
            self._publish_schema(src_schema, dst_schema)

    def _publish_schema(self, src_schema: str, dst_schema: str):
        if src_schema == dst_schema:
            raise GOBException("Publish schema: src and dst schema are the same. Don't understand what you want. "
                               "Really bad idea too though.")

        logger.info(f"Publish schema {dst_schema}")

        self._dst_datastore.drop_schema(dst_schema)
        self._dst_datastore.rename_schema(src_schema, dst_schema)

    def complete_prepare_process(self):
        """Function is called when all tasks are completed.

        Message contains summary of all actions, errors and warnings of all tasks.

        :return:
        """
        self.connect()
        self._publish_result_schemas()
        self.disconnect()

        metadata = {
            **self.header,
            **self.msg,  # Return original message in header
            "source_application": self.source_app,
            "destination_application": self.destination_app,
            "version": self._prepare_config['version'],
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

        result = {
            "header": metadata,
            "summary": {
                # Pass summary of import message.
                **self.msg['summary'],
            },
            "contents": [],
        }

        return result
