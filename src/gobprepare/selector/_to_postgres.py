from psycopg2.extras import Json


class ToPostgresSelector():

    def _create_destination_table(self, destination_table: dict):
        """Creates destination table

        :param destination_table:
        :return:
        """
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
