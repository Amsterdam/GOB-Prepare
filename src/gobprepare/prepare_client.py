"""
Contains the PrepareClient class.

The PrepareClient reads a dataset definition with a source and destination database and the actions
to perform on the data. For now the only supported action is a data clone from an Oracle to a Postgres
database.
"""

import datetime

from gobcore.database.connector.oracle import connect_to_oracle
from gobcore.database.connector.postgresql import connect_to_postgresql
from gobcore.database.reader.postgresql import list_tables_for_schema
from gobcore.database.writer.postgresql import drop_table, create_schema, execute_postgresql_query
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import PREPARE
from gobprepare.config import get_database_config
from gobprepare.cloner.oracle_to_postgres import OracleToPostgresCloner
from gobprepare.importers.api_importer import PostgresAPIImporter
from gobprepare.importers.csv_importer import PostgresCsvImporter
from gobprepare.selector.oracle_to_postgres import OracleToPostgresSelector
from gobprepare.selector.postgres_to_postgres import PostgresToPostgresSelector

READ_BATCH_SIZE = 100000
WRITE_BATCH_SIZE = 100000


class PrepareClient:
    """
    PrepareClient

    Construct using a prepare definition and call start_prepare_client() to start the data preparation.

    Handles everything from reading prepare definition to calling the appropriate helpers.
    """
    _src_connection = None
    _src_user = None
    _dst_connection = None
    _dst_user = None
    result = {}
    prepares_imports = None

    def __init__(self, prepare_config, msg):
        self.header = msg.get('header', {})
        self.msg = msg
        self._prepare_config = prepare_config
        self._actions = prepare_config['actions']
        self._name = prepare_config['name']
        self.source = self._prepare_config['source']
        self.source_app = self._prepare_config['source']['application']
        self.destination = self._prepare_config['destination']
        self.destination_app = self._prepare_config['destination']['application']
        self.publish_schemas = self._prepare_config.get('publish_schemas', {})

        assert isinstance(self.publish_schemas, dict)

        start_timestamp = int(datetime.datetime.utcnow().replace(microsecond=0).timestamp())
        self.process_id = self.header.get('process_id', f"{start_timestamp}.{self.source_app}{self._name}")

        self.header.update({
            'process_id': self.process_id,
            'source': self.source_app,
            'application': self.source.get('application'),
        })
        msg["header"] = self.header

        logger.configure(msg, "PREPARE")

    def connect(self):
        """Connects to data source and destination database

        :return:
        """
        self._src_connection, self._src_user = self._connect_application(self.source)
        self._dst_connection, self._dst_user = self._connect_application(self.destination)

    def _connect_application(self, application_config: dict):
        """Connects to application database

        :param application_config:
        :return:
        """
        if application_config['type'] == "oracle":
            connection, user = connect_to_oracle(get_database_config(application_config['application']))
        elif application_config['type'] == "postgres":
            connection, user = connect_to_postgresql(get_database_config(application_config['application']))
        else:
            raise NotImplementedError

        return connection, user

    def _close_connection(self, connection):
        if connection is not None:
            try:
                connection.close()
            except Exception:
                # Connection already closed?
                pass

    def disconnect(self):
        """Closes open database connections

        :return:
        """
        self._close_connection(self._src_connection)
        self._close_connection(self._dst_connection)

        self._src_connection = None
        self._dst_connection = None
        self._src_user = None
        self._dst_user = None

    def _get_cloner(self, action):
        if self.source['type'] == "oracle" and self.destination['type'] == "postgres":
            cloner = OracleToPostgresCloner(self._src_connection, action['source_schema'],
                                            self._dst_connection, action['destination_schema'],
                                            action)
        else:
            raise NotImplementedError
        return cloner

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
        if self.destination['type'] == "postgres":
            for schema in action['schemas']:
                logger.info(f"Clear schema {schema}")
                create_schema(self._dst_connection, schema)
                tables = list_tables_for_schema(self._dst_connection, schema)

                for table in tables:
                    full_tablename = f"{schema}.{table}"
                    drop_table(self._dst_connection, full_tablename)
        else:
            raise NotImplementedError

    def action_select(self, action: dict) -> int:
        """Select action. Selects data using query and inserts the data into destination database.

        :param action:
        :return:
        """
        if action['source'] == 'src' and self.source['type'] == "oracle" and self.destination['type'] == "postgres":
            selector = OracleToPostgresSelector(self._src_connection, self._dst_connection, action)
        elif action['source'] == 'dst' and self.destination['type'] == "postgres":
            selector = PostgresToPostgresSelector(self._dst_connection, self._dst_connection, action)
        else:
            raise NotImplementedError

        return selector.select()

    def action_execute_sql(self, action: dict):
        """Execute SQL action. Executes SQL on destination database.

        :param action:
        :return:
        """
        if self.destination['type'] == "postgres":
            query = self._get_query(action)
            execute_postgresql_query(self._dst_connection, query)
            logger.info(f"Executed query '{action['description']}' on Postgres")
        else:
            raise NotImplementedError

    def action_import_csv(self, action: dict):
        """Import CSV action. Import CSV into destination database

        :param action:
        :return:
        """
        if self.destination['type'] == "postgres":
            importer = PostgresCsvImporter(self._dst_connection, action)
        else:
            raise NotImplementedError

        rows_imported = importer.import_csv()
        logger.info(f"Imported {rows_imported} rows from CSV to table {action['destination']}")
        return rows_imported

    def action_import_api(self, action: dict):
        """Import API action. Import API into destination database action.

        :param action:
        :return:
        """
        if self.destination['type'] == "postgres":
            query = self._get_query(action)
            importer = PostgresAPIImporter(self._dst_connection, action, query)
        else:
            raise NotImplementedError

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
                "warnings": logger.get_warnings(),
                "errors": logger.get_errors(),
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
        logger.info(f"Prepare dataset {self._name} from {self.source_app} started")

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

        self.connect()
        self._run_prepare_action(action)
        self.disconnect()

        return self._get_result()

    def _publish_result_schemas(self):
        for src_schema, dst_schema in self.publish_schemas.items():
            self._publish_schema(src_schema, dst_schema)

    def _publish_schema(self, src_schema: str, dst_schema: str):
        if src_schema == dst_schema:
            raise GOBException(f"Publish schema: src and dst schema are the same. Don't understand what you want."
                               f"Really bad idea too though.")

        logger.info(f"Publish schema {dst_schema}")
        if self.destination['type'] == "postgres":
            execute_postgresql_query(self._dst_connection, f'DROP SCHEMA IF EXISTS "{dst_schema}" CASCADE')
            execute_postgresql_query(self._dst_connection, f'ALTER SCHEMA "{src_schema}" RENAME TO "{dst_schema}"')
        else:
            raise NotImplementedError

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
