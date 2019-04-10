"""
Contains the PrepareClient class.

The PrepareClient reads a dataset definition with a source and destination database and the actions
to perform on the data. For now the only supported action is a data clone from an Oracle to a Postgres
database.
"""

import datetime
import traceback

from gobcore.database.connector.oracle import connect_to_oracle
from gobcore.database.connector.postgresql import connect_to_postgresql
from gobcore.database.writer.postgresql import drop_schema, create_schema, execute_postgresql_query
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobprepare.cloner.oracle_to_postgres import OracleToPostgresCloner
from gobprepare.selector.oracle_to_postgres import OracleToPostgresSelector
from gobprepare.selector.postgres_to_postgres import PostgresToPostgresSelector
from gobprepare.config import get_database_config

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

        start_timestamp = int(datetime.datetime.utcnow().replace(microsecond=0).timestamp())
        self.process_id = f"{start_timestamp}.{self.source_app}{self._name}"
        extra_log_kwargs = {
            'job': self._name,
            'process_id': self.process_id,
            'application': self.source.get('application'),
            'actions': [action["type"] for action in self._actions]
        }

        # Log start of import process
        logger.set_name("PREPARE")
        logger.set_default_args(extra_log_kwargs)
        logger.info(f"Prepare dataset {self._name} from {self.source_app} started")

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

        logger.info(f"Connection to {application_config['application']} {user} has been made.")

        return connection, user

    def disconnect(self):
        """Closes open database connections

        :return:
        """
        if self._src_connection is not None:
            self._src_connection.close()
            self._src_connection = None
            self._src_user = None

        if self._dst_connection is not None:
            self._dst_connection.close()
            self._dst_connection = None
            self._dst_user = None

    def action_clone(self, action: dict) -> int:
        """Clones the source data using an external Cloner class.

        :return:
        """
        if self.source['type'] == "oracle" and self.destination['type'] == "postgres":
            cloner = OracleToPostgresCloner(self._src_connection, action['source_schema'],
                                            self._dst_connection, action['destination_schema'],
                                            action)
        else:
            raise NotImplementedError

        return cloner.clone()

    def action_clear(self, action: dict):
        """Clears destination database schema's (removes and recreates schemas)

        :param action:
        :return:
        """
        if self.destination['type'] == "postgres":
            for schema in action['schemas']:
                drop_schema(self._dst_connection, schema)
                logger.info(f"Drop schema {schema}")

                create_schema(self._dst_connection, schema)
                logger.info(f"Create schema {schema}")
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

    def prepare(self):
        """Starts the appropriate prepare action based on the input configuration

        :return:
        """
        self.result['actions'] = []
        for idx, action in enumerate(self._actions):
            self.result['actions'].append(self._run_prepare_action(action))

    def _run_prepare_action(self, action: dict):
        """Calls appropriate action.

        :param action:
        :return:
        """
        result = {}
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
        else:
            raise NotImplementedError

        result['action'] = action['type']
        return result

    def get_result(self):
        """Returns the result of the prepare job

        :return:
        """
        metadata = {
            **self.header,
            **self.msg,  # Return original message in header
            "process_id": self.process_id,
            "source_application": self.source_app,
            "destination_application": self.destination_app,
            "version": self._prepare_config['version'],
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        result = {
            "header": metadata,
            "summary": self.result,
        }
        return result

    def start_prepare_process(self):
        """Entry method. Handles the prepare process.

        :return:
        """
        try:
            self.connect()
            self.prepare()
            logger.info(f"Prepare job {self._name} finished")
        except Exception as e:
            # Print error message, the message that caused the error and a short stacktrace
            stacktrace = traceback.format_exc(limit=-5)
            print("Prepare failed: {e}", stacktrace)
            # Log the error and a short error description
            logger.error(f'Prepare failed: {e}')
        finally:
            self.disconnect()
