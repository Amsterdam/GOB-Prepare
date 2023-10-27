"""Contains the PrepareClient class.

The PrepareClient reads a dataset definition with a source and destination database and the actions
to perform on the data. For now the only supported action is a data clone from an Oracle to a Postgres
database.
"""


import datetime
from typing import Any, Literal, Optional, Union, cast

from gobconfig.datastore.config import get_datastore_config
from gobcore.datastore.factory import DatastoreFactory
from gobcore.datastore.oracle import OracleDatastore
from gobcore.datastore.sql import SqlDatastore
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import PREPARE
from pydash.arrays import flatten_deep

from gobprepare.cloner.oracle_to_postgres import OracleToPostgresCloner
from gobprepare.cloner.typing import ClonerConfig
from gobprepare.importers.api_importer import SqlAPIImporter
from gobprepare.importers.csv_importer import SqlCsvImporter
from gobprepare.importers.dump_importer import SqlDumpImporter
from gobprepare.importers.typing import APIImporterConfig, SqlCsvImporterConfig, SqlDumpImporterConfig
from gobprepare.selector.datastore_to_postgres import DatastoreToPostgresSelector
from gobprepare.selector.typing import SelectorConfig
from gobprepare.typing import (
    ClearConfig,
    CreateTableConfig,
    ExecuteSQLConfig,
    Message,
    PrepareMapping,
    PublishSchemasConfig,
    RowCountConfig,
    SQLBaseConfig,
    syncSchemaConfig,
    Summary,
    TaskList,
)
from gobprepare.utils.exceptions import DuplicateTableError
from gobprepare.utils.postgres import (
    create_table_columnar_as_query,
    check_table_existence_query,
    create_update_table_query,
    create_select_where_query
)


READ_BATCH_SIZE = 100000
WRITE_BATCH_SIZE = 100000

ActionConfig = dict[str, Any]


class PrepareClient:
    """PrepareClient.

    Construct using a prepare definition and call start_prepare_client() to start the data preparation.

    Handles everything from reading prepare definition to calling the appropriate helpers.
    """

    result: dict[str, Any] = {}
    prepares_imports = None
    _src_datastore: Optional[OracleDatastore] = None
    _dst_datastore: Optional[SqlDatastore] = None

    def __init__(self, prepare_config: PrepareMapping, msg: Message) -> None:
        self.header = msg.get("header", {})
        self.msg = msg
        self._prepare_config = prepare_config
        self._actions = prepare_config["actions"]
        self._name = prepare_config["name"]
        self.source = self._prepare_config.get("source")
        self.source_app = self._prepare_config.get("source", {}).get("application")
        self.destination = self._prepare_config["destination"]
        self.destination_app = self._prepare_config["destination"]["application"]
        self.publish_schemas = self._prepare_config.get("publish_schemas", {})

        assert isinstance(self.publish_schemas, dict)

        self.header.update(
            {
                "source": self.source_app,
                "application": self.source_app,
            }
        )
        msg["header"] = self.header

        self._set_datastores()

    def _set_datastores(self) -> None:
        if self.source:
            self._src_datastore = DatastoreFactory.get_datastore(
                get_datastore_config(self.source["application"]), self.source.get("read_config", {})
            )

            # This class only supports OracleDatastore as source
            assert isinstance(
                self._src_datastore, OracleDatastore
            ), "Only Oracle Datastore is currently supported as src"

        self._dst_datastore = DatastoreFactory.get_datastore(
            get_datastore_config(self.destination["application"]), self.destination.get("read_config", {})
        )

        # This class assumes SqlDatastore as destination.
        assert isinstance(self._dst_datastore, SqlDatastore), "Only Sql Datastores are currently supported as dst"

    def connect(self) -> None:
        """Connect to data source and destination database.

        :return:
        """
        if self._src_datastore:
            self._src_datastore.connect()
        self._dst_datastore.connect()  # type: ignore[union-attr]

    def disconnect(self) -> None:
        """Close open database connections.

        :return:
        """
        if self._src_datastore:
            self._src_datastore.disconnect()
        self._dst_datastore.disconnect()  # type: ignore[union-attr]
        self._src_datastore = None
        self._dst_datastore = None

    def _get_cloner(self, action: ClonerConfig) -> OracleToPostgresCloner:
        # Assert that src datastore is set. Dst datastore is always present (would have failed at initialisation otw)
        assert self._src_datastore is not None, "No src datastore set"

        return OracleToPostgresCloner(
            self._src_datastore, action["source_schema"], self._dst_datastore, action["destination_schema"], action
        )

    def action_clone(self, action: ClonerConfig) -> int:
        """Clone the source data using an external Cloner class.

        :return:
        """
        cloner = self._get_cloner(action)
        return cloner.clone()

    def action_clear(self, action: ClearConfig) -> None:
        """Clear destination database schema's (removes and recreates schemas).

        :param action:
        :return:
        """
        for schema in action["schemas"]:
            logger.info(f"Clear schema {schema}")
            self._dst_datastore.create_schema(schema)  # type: ignore[union-attr]
            tables = self._dst_datastore.list_tables_for_schema(schema)  # type: ignore[union-attr]

            for table in tables:
                full_tablename = f"{schema}.{table}"
                self._dst_datastore.drop_table(full_tablename)  # type: ignore[union-attr]

    def action_select(self, action: SelectorConfig) -> int:
        """Select action. Selects data using query and inserts the data into destination database.

        :param action:
        :return:
        """
        if action["source"] == "src":
            assert self._src_datastore is not None, "src datastore not set"

        selector = DatastoreToPostgresSelector(
            # Select from src_datastore or dst_datastore
            self._src_datastore if action["source"] == "src" else self._dst_datastore,
            self._dst_datastore,
            action,
        )

        return selector.select()

    def action_execute_sql(self, action: ExecuteSQLConfig) -> None:
        """Execute SQL action. Executes SQL on destination database.

        :param action:
        :return:
        """
        self._dst_datastore.execute(self._get_query(action))  # type: ignore[union-attr]
        logger.info(f"Executed query '{action['description']}' on destination")

    def action_import_csv(self, action: SqlCsvImporterConfig) -> int:
        """Import CSV action. Import CSV into destination database.

        :param action:
        :return:
        """
        importer = SqlCsvImporter(self._dst_datastore, action)

        rows_imported = importer.import_csv()
        logger.info(f"Imported {rows_imported:,} rows from CSV to table {action['destination']}")
        return rows_imported

    def action_import_sql_dump(self, action: SqlDumpImporterConfig) -> str:
        """Import SQL dump action. Import SQL dump into destination database.

        :param action:
        :return: number of files imported
        """
        importer = SqlDumpImporter(self._dst_datastore, action)

        file_imported = importer.import_dump()
        logger.info(f"Dump {file_imported} successfully imported.")
        return file_imported

    def action_create_table(self, action: CreateTableConfig) -> None:
        """Create destination table."""
        query = self._get_query(action)
        create_query = create_table_columnar_as_query(self._dst_datastore, action["table_name"], query)
        self._dst_datastore.execute(create_query)  # type: ignore[union-attr]
        self._dst_datastore.execute(f"ANALYZE {action['table_name']}")  # type: ignore[union-attr]

        logger.info(f"Created table '{action['table_name']}'")

    def action_import_api(self, action: APIImporterConfig) -> int:
        """Import API action. Import API into destination database action.

        :param action:
        :return:
        """
        query = self._get_query(action)
        importer = SqlAPIImporter(self._dst_datastore, action, query)

        rows_imported = importer.import_api()
        logger.info(f"Imported {rows_imported} rows from API to table {action['destination']}")
        return rows_imported

    def action_publish_schemas(self, action: PublishSchemasConfig) -> list[str]:
        """Return list with publish schemas."""
        for src_schema, dst_schema in action["publish_schemas"].items():
            self._publish_schema(src_schema, dst_schema)
        return list(action["publish_schemas"].values())

    def action_check_row_counts(self, action: RowCountConfig) -> bool:
        """Check import action. Verify number of rows in result tables.

        :param action:
        :return:
        """
        tables_counts_ok = True
        for table, expected_count in action["table_row_counts"].items():
            row_count = self._dst_datastore.table_count(table)  # type: ignore[union-attr]
            deviation = ((row_count / expected_count) - 1) * 100
            if abs(deviation) > action["margin_percentage"]:
                logger.error(
                    f"Deviation of {deviation:.2f}% for {table}! Expected {expected_count} rows, got {row_count}"
                )
                tables_counts_ok = False
            else:
                logger.info(
                    f"Row count for table {table} succeeded:"
                    f" expected {expected_count} rows, got {row_count}; deviation of {deviation:.2f}%"
                )
        return tables_counts_ok

    def action_check_source_sync_complete(self, action: syncSchemaConfig) -> bool:
        """Check import action. Verify when sync tables from databricks is completed.

        :param action:
        :return:
        """

        public_schema = action["public_schema"]
        table_name = action["table_name"]
        schema = action["schema"]
        if not self._dst_datastore.query(check_table_existence_query(public_schema, table_name)):
            raise GOBException(f"Table '{public_schema}.{table_name}' does not exists.")
        else:
            try:
                where_conditions = [{ "sync_schema": schema }]
                select_query = create_select_where_query(table_name, where_conditions)
                record = next(self._dst_datastore.query(select_query))
            except StopIteration:
                raise GOBException(f"No record for schema '{schema}' found in '{public_schema}.{table_name}'.")
            else:
                self._execute_update_sync_schema(schema, table_name, record, update_action = 'start_prepare')

    def action_complete_prepare(self, action: syncSchemaConfig) -> None:
        """update sync table after prepare is completed.

        :param action:
        :return:
        """
        public_schema = action["public_schema"]
        table_name = action["table_name"]
        schema = action["schema"]
        try:
            where_conditions = [{ "sync_schema": schema }]
            select_query = create_select_where_query(table_name, where_conditions)
            record = next(self._dst_datastore.query(select_query))
        except StopIteration:
            raise GOBException(f"No record for schema '{schema}' found in '{public_schema}.{table_name}'.")
        else:
            self._execute_update_sync_schema(schema, table_name, record, update_action = 'end_prepare')

    def _execute_update_sync_schema(self, schema, table_name, record: [str], update_action: str) -> None:
        last_successful_sync = record[1]
        last_sync_start = record[2]
        # start prepare
        if update_action == 'start_prepare':
            if last_successful_sync == None:
                raise GOBException(f"Prepare processing for '{schema}' can not be started."
                                f"Databricks sync for schema '{schema}' not completed yet."
                                f"Last sync job started at '{last_sync_start}'")

            update_list = [{
                "last_prepare_start": "current_timestamp",
                "last_prepare_end": "NULL"
            }]
            logger.info(f"START: Prepare processing for '{schema}' started.")
        # end prepare
        elif update_action == 'end_prepare':
            update_list = [{
                "last_prepare_end": "CURRENT_TIMESTAMP"
            }]
            logger.info(f"END: Prepare processing for '{schema}' completed.")

        where_conditions = [{"sync_schema": schema}]
        update_query = create_update_table_query(table_name, update_list, where_conditions)
        self._dst_datastore.execute(update_query)

    def _get_query(self, action: SQLBaseConfig) -> str:
        """Extract query from action. Reads query from action or from file.

        :param action:
        :return:
        """
        src = action.get("query_src")

        if src == "string":
            # Multiline queries are represented as lists in JSON. Join list as string
            if isinstance(action["query"], list):
                return "\n".join(action["query"])
            return action["query"]
        elif src == "file":
            with open(action["query"]) as f:
                return f.read()

        raise GOBException("Missing or invalid 'query_src'")

    def _run_prepare_action(self, action: ActionConfig) -> Optional[dict[str, str]]:  # noqa: C901
        """Call appropriate action.

        :param action:
        :return:
        """
        result = {
            "id": action["id"],
            "action": action["type"],
        }
        # Use cast to let the action configuration TypedDict do its thing.
        if action["type"] == "clone":
            result["rows_copied"] = self.action_clone(cast(ClonerConfig, action))
        elif action["type"] == "clear":
            self.action_clear(cast(ClearConfig, action))
            result["clear"] = "OK"
        elif action["type"] == "select":
            result["rows_copied"] = self.action_select(cast(SelectorConfig, action))
        elif action["type"] == "execute_sql":
            self.action_execute_sql(cast(ExecuteSQLConfig, action))
            result["executed"] = "OK"
        elif action["type"] == "create_table":
            self.action_create_table(cast(CreateTableConfig, action))
            result["executed"] = "OK"
        elif action["type"] == "import_csv":
            result["rows_imported"] = self.action_import_csv(cast(SqlCsvImporterConfig, action))
        elif action["type"] == "import_api":
            result["rows_imported"] = self.action_import_api(cast(APIImporterConfig, action))
        elif action["type"] == "import_dump":
            result["file_imported"] = self.action_import_sql_dump(cast(SqlDumpImporterConfig, action))
        elif action["type"] == "check_row_counts":
            result["row_count_check"] = self.action_check_row_counts(cast(RowCountConfig, action))
        elif action["type"] == "check_source_sync_complete":
            result["check_source_sync"] = self.action_check_source_sync_complete(cast(syncSchemaConfig, action))
        elif action["type"] == "join_actions":
            # Action only joins dependencies. No further actions necessary
            return None
        elif action["type"] == "publish_schemas":
            result["published_schemas"] = self.action_publish_schemas(cast(PublishSchemasConfig, action))
        elif action["type"] == "complete_prepare":
            result["complete_prepare"] = self.action_complete_prepare(cast(syncSchemaConfig, action))
        else:
            raise NotImplementedError

        self.result["action"] = result
        return result

    def _get_result(self) -> Message:
        metadata: dict[str, Any] = {
            **self.header,
            **self.msg,  # Return original message in header
            "source_application": self.source_app,
            "destination_application": self.destination_app,
            "version": self._prepare_config["version"],
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }

        result: Message = {
            "header": metadata,
            "summary": {
                **self.result,
                **logger.get_summary(),
            },
        }
        return result

    def _split_clone_action(self, action: ClonerConfig) -> TaskList:
        """Split a clone action into smaller tasks. Create a task per table.

        :param action:
        :return:
        """
        self.connect()
        cloner = self._get_cloner(action)
        table_names = cloner.read_source_table_names()
        self.disconnect()

        tasks: TaskList = []
        for table_name in table_names:
            tasks.append(
                {
                    "task_name": action["id"] + "__" + table_name.lower(),
                    "dependencies": action.get("depends_on", []),
                    "extra_msg": {
                        "override": {
                            # Include only this table. Unset the ignore list
                            "include": [f"^{table_name}$"],
                            "ignore": [],
                        },
                        "original_action": action["id"],
                    },
                }
            )

        dependencies = [task["task_name"] for task in tasks]

        # Create join action with dependencies on new steps
        tasks.append(
            {
                "task_name": action["id"],
                "dependencies": dependencies,
                "extra_msg": {"override": {"type": "join_actions"}},
            }
        )

        return tasks

    def _create_final_tasks(self, actions: list[ActionConfig]) -> TaskList:
        """Create final tasks from actions that are depending on all other actions.

        Add leaf nodes as dependencies for final tasks
        """
        final_tasks = [action for action in actions if action.get("depends_on") == "*"]
        all_dependencies = flatten_deep(
            [action.get("depends_on", []) for action in self._actions if action.get("depends_on") != "*"]
        )

        # Find the leaf nodes of the task graph.
        # Leaf nodes are nodes where no other nodes depend on. We add the final tasks after those nodes.
        action_ids_no_childs = [action["id"] for action in self._actions if action["id"] not in all_dependencies]

        return [
            {
                "task_name": final_task["id"],
                "dependencies": [action_id for action_id in action_ids_no_childs if action_id != final_task["id"]],
            }
            for final_task in final_tasks
        ]

    def _create_tasks(self) -> TaskList:
        """Create tasks to be put on the message bus from the actions defined in the config.

        :return:
        """
        tasks: TaskList = []
        for action in self._actions:
            if action["type"] == "clone":
                # Split a clone action into smaller tasks
                tasks.extend(self._split_clone_action(cast(ClonerConfig, action)))
            elif action.get("depends_on") == "*":
                # Depends on all other tasks, will be handles separately below
                continue
            else:
                tasks.append(
                    {
                        "task_name": action["id"],
                        "dependencies": action.get("depends_on", []),
                    }
                )
        final_tasks = self._create_final_tasks(self._actions)

        return tasks + final_tasks

    def _get_task_message(self, tasks) -> Message:
        """Publish tasks for further processing.

        :param tasks:
        :return:
        """
        msg: Message = {
            "header": {
                **self.header,
                "extra": {
                    "catalogue": self.header["catalogue"],
                },
            },
            "contents": {
                "tasks": tasks,
                "key_prefix": PREPARE,
            },
        }
        return msg

    def start_prepare_process(self) -> Message:
        """Entry method. Starts the prepare process.

        :return:
        """
        app_msg = f" from {self.source_app}" if self.source_app else ""
        logger.info(f"Prepare dataset {self._name}{app_msg} started")

        tasks = self._create_tasks()
        return self._get_task_message(tasks)

    def run_prepare_task(self) -> Union[Message, Literal[False]]:
        """Run incoming task.

        Checks if id is known in configuration. If not, we may have generated this task. In that case original_action
        will be set, possibly with override set.
        In that case, we copy the original action and use the override dict to update the action.

        :return:
        """
        task_name = self.header["task_name"]
        action = [action for action in self._actions if action["id"] == task_name]

        if not action and "original_action" in self.msg:
            action = [action for action in self._actions if action["id"] == self.msg["original_action"]]

        if not action:
            raise GOBException(f"Unknown action with id {task_name}")

        first_action = action[0]

        if "override" in self.msg:
            first_action.update(self.msg["override"])

        self.connect()

        try:
            self._run_prepare_action(first_action)
        except DuplicateTableError as err:
            print(f"WARNING: {err}, ignoring duplicate for task '{task_name}'")
            return False
        else:
            return self._get_result()
        finally:
            self.disconnect()

    def _publish_schema(self, src_schema: str, dst_schema: str) -> None:
        if src_schema == dst_schema:
            raise GOBException(
                "Publish schema: src and dst schema are the same. Don't understand what you want. "
                "Really bad idea too though."
            )

        logger.info(f"Publish schema {dst_schema}")

        self._dst_datastore.drop_schema(dst_schema)  # type: ignore[union-attr]
        self._dst_datastore.rename_schema(src_schema, dst_schema)  # type: ignore[union-attr]

    def complete_prepare_process(self) -> Summary:
        """Return summary of all tasks actions, errors and warnings.

        Function is called when all tasks are completed.
        Message contains summary of all actions, errors and warnings of all tasks.

        :return:
        """
        metadata: dict[str, Any] = {
            **self.header,
            **self.msg,  # Return original message in header
            "source_application": self.source_app,
            "destination_application": self.destination_app,
            "version": self._prepare_config["version"],
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }

        result = {
            "header": metadata,
            "summary": {
                # Pass summary of import message.
                **self.msg["summary"],
            },
            "contents": [],
        }

        return result
