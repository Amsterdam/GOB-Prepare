from gobcore.database.reader.oracle import query_oracle


class FromOracleSelector():

    def _read_rows(self, query):
        return query_oracle(self._src_connection, [query])
