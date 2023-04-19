from typing import Literal

from psycopg2.extras import Json

from gobprepare.selector.typing import DestinationTable
from gobprepare.utils.exceptions import DuplicateTableError
from gobprepare.utils.postgres import create_table_columnar_query


class ToPostgresSelector:
    """To Postgres Selector."""

    def _create_destination_table(self, destination_table: DestinationTable) -> Literal[True]:
        """Create destination table.

        :param destination_table:
        :return:
        """
        schema, table = destination_table["name"].split(".")

        if table in self._dst_datastore.list_tables_for_schema(schema):  # type: ignore[attr-defined]
            raise DuplicateTableError(f"Table already exists: {table} ({schema})")

        columns = ",".join([f"{column['name']} {column['type']} NULL" for column in destination_table["columns"]])
        create_query = create_table_columnar_query(
            self._dst_datastore, destination_table["name"], columns  # type: ignore[attr-defined]
        )
        self._dst_datastore.execute(create_query)  # type: ignore[attr-defined]
        return True

    def _prepare_row(self, row: list[str], columns: list[dict[str, str]]) -> list[str]:
        """Perform data transformations where necessary.

        :param row:
        :param columns:
        :return:
        """
        for idx, val in enumerate(row):
            if columns[idx]["type"] in ["JSON", "JSONB"]:
                row[idx] = Json(val)
        return row

    def _write_rows(self, table: str, values: list[list[str]]) -> int:
        return self._dst_datastore.write_rows(table, values)  # type: ignore[attr-defined, no-any-return]
