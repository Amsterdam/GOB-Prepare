"""
OracleToPostgresSelector class

Contains logic to run a (select) query on an oracle database and import this in a new table in Postgres.
"""
import itertools

from gobcore.database.reader.oracle import query_oracle
from gobcore.database.writer.postgresql import execute_postgresql_query, write_rows_to_postgresql
from gobcore.logging.logger import logger


class OracleToPostgresSelector():
    WRITE_BATCH_SIZE = 50000

    def __init__(self, oracle_connection, postgres_connection, config: dict):
        """
        :param oracle_connection:
        :param postgres_connection:
        :param config:
        """
        self._src_connection = oracle_connection
        self._dst_connection = postgres_connection
        self._config = config
        self.queries = config.get('queries', [])

    def select(self) -> int:
        """Entry method. Loops through queries and calls _select() for every query

        :return:
        """
        result = 0
        for query in self.queries:
            result += self._select("\n".join(query['query']), query['destination_table'])
        return result

    def _select(self, query: str, destination_table: dict) -> int:
        """Run for every query. Creates destination table if desired and saves result of select query in destination
        table.

        :param query:
        :param destination_table:
        :return:
        """
        if destination_table.get('create', False):
            self._create_destination_table(destination_table)

        rows = query_oracle(self._src_connection, [query])
        total_cnt = 0

        while True:
            chunk = itertools.islice(rows, self.WRITE_BATCH_SIZE)
            values = [[row[column["name"].lower()] for column in destination_table['columns']] for row in chunk]
            write_rows_to_postgresql(self._dst_connection, destination_table['name'], values)

            total_cnt += len(values)

            if len(values) < self.WRITE_BATCH_SIZE:
                logger.info(f"Written {total_cnt} rows to destination table {destination_table['name']}")
                return total_cnt

    def _create_destination_table(self, destination_table: dict):
        """Creates destination table

        :param destination_table:
        :return:
        """
        columns = ','.join([f"{column['name']} {column['type']} NULL" for column in destination_table['columns']])
        create_query = f"CREATE TABLE {destination_table['name']} ({columns})"
        execute_postgresql_query(self._dst_connection, create_query)
