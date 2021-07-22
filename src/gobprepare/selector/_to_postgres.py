from psycopg2.extras import Json
from gobprepare.utils.exceptions import DuplicateTableError


class ToPostgresSelector():

    def _create_destination_table(self, destination_table: dict):
        """Creates destination table

        :param destination_table:
        :return:
        """
        schema, table = destination_table['name'].split('.')

        if table in self._dst_datastore.list_tables_for_schema(schema):
            raise DuplicateTableError(f"Table already exists: {table} ({schema})")

        columns = ','.join([f"{column['name']} {column['type']} NULL" for column in destination_table['columns']])
        create_query = f"CREATE TABLE {destination_table['name']} ({columns})"
        self._dst_datastore.execute(create_query)

    def _prepare_row(self, row: list, columns: list):
        """Perform data transformations where necessary

        :param row:
        :param columns:
        :return:
        """
        for idx, val in enumerate(row):
            if columns[idx]['type'] in ["JSON", "JSONB"]:
                row[idx] = Json(val)
        return row

    def _write_rows(self, table: str, values):
        self._dst_datastore.write_rows(table, values)
