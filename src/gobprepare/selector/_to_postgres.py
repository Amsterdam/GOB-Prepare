from gobcore.database.writer.postgresql import execute_postgresql_query, write_rows_to_postgresql
from psycopg2.extras import Json


class ToPostgresSelector():

    def _create_destination_table(self, destination_table: dict):
        """Creates destination table

        :param destination_table:
        :return:
        """
        columns = ','.join([f"{column['name']} {column['type']} NULL" for column in destination_table['columns']])
        create_query = f"CREATE TABLE {destination_table['name']} ({columns})"
        execute_postgresql_query(self._dst_connection, create_query)

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
        write_rows_to_postgresql(self._dst_connection, table, values)
