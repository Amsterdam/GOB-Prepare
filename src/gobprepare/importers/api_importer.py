import itertools
from collections.abc import Iterable, KeysView
from typing import Iterator, Optional

from gobcore.datastore.sql import SqlDatastore
from gobcore.exceptions import GOBException

from gobprepare.config import GOB_API_HOST, GRAPHQL_ENDPOINT, STREAMING_GRAPHQL_ENDPOINT, WRITE_BATCH_SIZE
from gobprepare.importers.typing import APIImporterConfig
from gobprepare.utils.graphql import GraphQL, GraphQLStreaming
from gobprepare.utils.sql import get_create_table_sql, get_full_table_name
from gobprepare.utils.typing import GraphQLEntities, MetaField


class SqlAPIImporter:
    """Imports a JSON from GOB API to a Postgres table."""

    def __init__(self, dst_datastore: SqlDatastore, config: APIImporterConfig, query: str) -> None:
        """Initialise SqlAPIImporter."""
        self._dst_datastore = dst_datastore
        self.query = query
        self.schema = config["schema"]
        self.table_name = config["destination"]
        self.description = config["description"]
        self.meta_type = config["meta_type"]

    def import_api(self) -> int:
        """Make API calls and import data into the database.

        Make call to GraphQL Streaming API to fetch data using query.
        Make call to GraphQL API to fetch meta data. Meta data will be using to create
        database table.

        Import data into the Postgres database.

        :return: Number of records imported.
        """
        entities = self.get_entities()
        meta_data = self.get_meta_data()
        return self.import_data(entities, meta_data)

    def get_entities(self) -> GraphQLStreaming:
        """Get entities from GraphQL API.

        :return: Iterator to entities.
        """
        return GraphQLStreaming(GOB_API_HOST, STREAMING_GRAPHQL_ENDPOINT, self.query)

    def get_meta_data(self) -> GraphQL:
        """Get meta data from GraphQL API.

        :return: Iterator to meta data.
        """
        meta_query = self.get_meta_query(self.meta_type)
        return GraphQL(GOB_API_HOST, GRAPHQL_ENDPOINT, meta_query)

    def import_data(self, entities: GraphQLStreaming, meta_data: GraphQL) -> int:
        """Re-create table and import data into the Postgres database.

        :param entities: Entities to import.
        :param meta_data: Meta data to create a table.
        :return: Number of records imported.
        """
        self._dst_datastore.create_schema(self.schema)

        # clone entities to get field name from the first record
        entities_clone, entities_iter = itertools.tee(entities, 2)
        field_names = self.get_field_names(entities_clone)
        meta_fields = self.get_meta_fields(meta_data)
        self.create_table(meta_fields, field_names)
        return self.import_entities(entities_iter, field_names)

    @staticmethod
    def get_meta_query(meta_type: str) -> str:
        """Construct meta data GraphQL query for a certain entity (meta_type).

        :param meta_type: GraphQL meta type name.
        :return: Meta data GraphQL query.
        """
        return (
            """
{
  __type(name: "%s") {
    name
    fields {
      name
      description
      type {
        name
      }
    }
  }
}
"""
            % meta_type
        )

    def get_meta_fields(self, meta_data: GraphQL) -> dict[str, MetaField]:
        """Get meta fields from meta data.

        :param meta_data: Meta data.
        :return: Dict of meta fields.
        """
        meta_fields = {}
        for meta in meta_data:
            meta_fields.update({field["name"]: field for field in meta["data"]["__type"]["fields"]})
        return meta_fields

    def get_field_names(self, entities: Iterator[GraphQLEntities]) -> KeysView[str]:
        """Get field names from a first data record.

        :param entities: Data entities.
        :return: List of field names.
        """
        first_record: Optional[GraphQLEntities] = next(entities, None)
        if not first_record:
            raise GOBException("No data available.")
        return first_record["node"].keys()

    def create_table(self, meta_fields: dict[str, MetaField], field_names: Iterable[str]) -> None:
        """Create table using field names and meta information.

        :param meta_fields: Dict of meta fields.
        :param field_names: List of field names.
        :return: None.
        """
        create_table_sql = get_create_table_sql(
            schema=self.schema,
            table_name=self.table_name,
            description=self.description,
            meta_fields=meta_fields,
            field_names=field_names,
        )
        self._dst_datastore.execute(create_table_sql)

    def import_entities(self, entities: Iterator[GraphQLEntities], field_names: Iterable[str]) -> int:
        """Import entities to the database using batch INSERT.

        :param entities: Data entities.
        :param field_names: List of field names.
        :return: Number of records imported.
        """
        rows = []
        counter = 0
        total_counter = 0
        for entity in entities:
            rows.append(self.get_row_values(entity["node"], field_names))
            counter += 1
            total_counter += 1
            if counter >= WRITE_BATCH_SIZE:
                self.write_rows(rows)
                rows = []
                counter = 0
        self.write_rows(rows)
        return total_counter

    @staticmethod
    def get_row_values(node: dict[str, str], field_names: Iterable[str]) -> list[str]:
        """Get list of row values for an entity node (which is expected to be a dict).

        :param node: Dict with entity data.
        :param field_names: List of field names.
        :return: List of row values.
        """
        return [node[field_name] for field_name in field_names]

    def write_rows(self, rows: list[list[str]]) -> None:
        """Insert rows to a Postgres database.

        :param rows: List of list of row values.
        :return: None.
        """
        self._dst_datastore.write_rows(get_full_table_name(self.schema, self.table_name), rows)
