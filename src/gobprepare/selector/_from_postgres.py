from gobcore.database.reader.postgresql import query_postgresql


class FromPostgresSelector():

    def _read_rows(self, query):
        return query_postgresql(self._src_connection, query)
