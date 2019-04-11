from gobcore.database.writer.postgresql import execute_postgresql_query, write_rows_to_postgresql


class ToPostgresSelector():

    def _create_destination_table(self, destination_table: dict):
        """Creates destination table

        :param destination_table:
        :return:
        """
        columns = ','.join([f"{column['name']} {column['type']} NULL" for column in destination_table['columns']])
        create_query = f"CREATE TABLE {destination_table['name']} ({columns})"
        execute_postgresql_query(self._dst_connection, create_query)

    def _write_rows(self, table: str, values):
        write_rows_to_postgresql(self._dst_connection, table, values)
