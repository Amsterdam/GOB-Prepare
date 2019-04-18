"""
Contains the OracleToPostgresCloner class, which contains the logic to clone an Oracle database schema to a Postgres
database.
"""
import itertools
import time
from math import ceil
from typing import Dict, List, Tuple

from gobprepare.config import DEBUG
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.database.reader.oracle import read_from_oracle, query_oracle
from gobprepare.cloner.mapping.oracle_to_postgres import \
    get_postgres_column_definition
from gobcore.database.writer.postgresql import (drop_table, execute_postgresql_query,
                                                write_rows_to_postgresql)


class OracleToPostgresCloner():
    READ_BATCH_SIZE = 100000
    WRITE_BATCH_SIZE = 100000

    schema_definition = None
    _mask_columns = {}
    _ignore_tables = []
    _include_tables = []

    def __init__(self, oracle_connection, src_schema: str, postgres_connection, dst_schema: str, config: dict):
        """
        :param oracle_connection:
        :param src_schema:
        :param postgres_connection:
        :param dst_schema:
        :param config:
        """
        self._src_connection = oracle_connection
        self._src_schema = src_schema
        self._dst_connection = postgres_connection
        self._dst_schema = dst_schema

        if config is not None:
            if config.get('ignore') and config.get('include'):
                raise GOBException("Don't know what to do with both ignore and include in the config. "
                                   "Use either, not both.")
            self._mask_columns = config.get('mask', {})
            self._ignore_tables = config.get('ignore', [])
            self._include_tables = config.get('include', [])

    def _read_source_table_names(self) -> list:
        """Returns a list of table names present in given schema

        :return:
        """

        def quote_string(string):
            return f"'{string}'"

        if self._ignore_tables:
            table_select = f" AND table_name NOT IN " \
                f"({','.join([quote_string(table) for table in self._ignore_tables])})"
        elif self._include_tables:
            table_select = f" AND table_name IN ({','.join([quote_string(table) for table in self._include_tables])})"
        else:
            table_select = ""

        query = f"SELECT table_name FROM all_tables WHERE owner='{self._src_schema}'{table_select} ORDER BY table_name"
        table_names = read_from_oracle(self._src_connection, [query])
        return [row['table_name'] for row in table_names]

    def _get_source_table_definition(self, table: str) -> List[Tuple[str, str]]:
        """
        Returns the column definitions for given table. The result is a list of 2-tuples, where each
        tuple represents a column. The first element of the tuple contains the column name, the
        second element the string representation of the column definition (as you would use in a
        create query).

        For example: [(first_name, VARCHAR(20)), (age, SMALLINT), (birthday, DATE)]

        :param table:
        :return:
        """
        query = f"SELECT column_name, data_type, data_length, data_precision, data_scale " \
            f"FROM all_tab_columns WHERE owner='{self._src_schema}' AND table_name='{table}'"
        columns = read_from_oracle(self._src_connection, [query])

        table_definition = [
            (
                column['column_name'],
                get_postgres_column_definition(
                    column["data_type"],
                    column["data_length"],
                    column["data_precision"],
                    column["data_scale"],
                )
            ) for column in columns
        ]
        return table_definition

    def _prepare_destination_database(self) -> None:
        """Creates tables in the destination schema

        :return:
        """
        schema_definition = self._get_destination_schema_definition()
        for table_definition in schema_definition:
            self._create_destination_table(table_definition)

        logger.info(f"Destination database schema {self._dst_schema} with tables created")

    def _create_destination_table(self, table_definition: Tuple[str, List]) -> None:
        """
        Creates table in destination database based on table_definition. See _get_table_definition
        for the table_definition format.

        :param table_definition:
        :return:
        """
        table_name, table_columns = table_definition
        # Create column definitions
        columns = ','.join([f"{cname} {ctype} NULL" for cname, ctype in table_columns])
        create_query = f"CREATE TABLE {self._dst_schema}.{table_name} ({columns})"

        drop_table(self._dst_connection, f"{self._dst_schema}.{table_name}")
        execute_postgresql_query(self._dst_connection, create_query)

    def _get_destination_schema_definition(self) -> List[Tuple[str, List[Tuple[str, str]]]]:
        """
        Returns a (very simple) schema definition, containing only the column data types for each
        table in the schema.

        The result is a list of tuples (table_name, table_definition), where the table_definition is
        of the form as defined in _get_table_definition.

        :param connection:
        :param schema:
        :return:
        """

        if not self.schema_definition:
            table_names = self._read_source_table_names()
            self.schema_definition = [
                (table_name, self._get_source_table_definition(table_name))
                for table_name in table_names
            ]

        return self.schema_definition

    def _copy_data(self) -> int:
        """
        Copies data from source database to destination database

        :return:
        """
        rows_copied = 0
        for table_definition in self._get_destination_schema_definition():
            rows_copied += self._copy_table_data(table_definition)

        return rows_copied

    def _copy_table_data(self, table_definition: Tuple[str, List]) -> int:
        """
        Copies table data from source database to destination database

        :param table_definition:
        :return:
        """
        table_name, column_definitions = table_definition
        full_table_name = f"{self._src_schema}.{table_name}"

        """Generates a query with a sub-select. The inner query selects all columns from the source table. The outer
        query performs the transformations (such as TO_WKTGEOMETRY). Reason for this construction is that Oracle
        somehow performs very poorly when a OFFSET ... FETCH selection is done for larger OFFSETs in combination
        with the TO_WKTGEOMETRY function. For example, the performance is reasonable for:

        SELECT SDO_UTIL.TO_WKTGEOMETRY(GEO) AS GEO FROM SOME_TABLE OFFSET 0 ROWS FETCH FIRST 100000 ROWS ONLY

        But when we increase the offset the execution time grows exponentially (say 15s for OFFSET 0, 25s for OFFSET
        50000, 45s for OFFSET 100000 and so on). This means we'll never reach the end of the table.

        Somehow Oracle treats the query with sub-select different than its childless counterpart, although the
        resulting query plans are the same. Something goes wrong when a call to WKT_TOGEOMETRY is done to a row high
        in the (intermediate) result set.
        """
        outer_select = self._get_select_list_for_table_definition(table_definition)
        inner_select = ','.join([column_name for column_name, column_type in column_definitions])

        query = f"SELECT /*+ PARALLEL */ {outer_select} FROM (SELECT {inner_select} FROM {full_table_name})"
        cursor = query_oracle(self._src_connection, [query])

        row_cnt = 0
        while True:
            read_time = time.time()
            chunk = list(itertools.islice(cursor, self.READ_BATCH_SIZE))
            read_time = time.time() - read_time
            row_cnt += len(chunk)

            write_start = time.time()
            self._insert_rows(table_definition, chunk)
            write_time = time.time() - write_start

            if DEBUG:
                logger.info("Total: {:>8.1f}s  Read: {:>8.1f}s  Write: {:>8.1f}s  Length: {:>8} rows  ({})".format(
                    read_time + write_time,
                    read_time,
                    write_time,
                    len(chunk),
                    table_name
                ))

            if len(chunk) < self.READ_BATCH_SIZE:
                # We're done
                logger.info(f"Written {row_cnt} rows to destination table {full_table_name}")
                return row_cnt

    def _mask_rows(self, table_name: str, row_data: List[Dict]) -> List[Dict]:
        """
        Masks the provided rows if a mask is defined in the config.

        :param table_name:
        :param row_data:
        :return:
        """
        if table_name not in self._mask_columns:
            return row_data

        for row in row_data:
            row.update(self._mask_columns[table_name])
        return row_data

    def _insert_rows(self, table_definition: Tuple[str, List], row_data: List[Dict]) -> None:
        """
        Inserts row_data into table. Input is a list of dicts with column: value pairs.

        Data is inserted in chunks of WRITE_BATCH_SIZE.

        :param table_definition:
        :param row_data:
        :return:
        """
        table_name, table_columns = table_definition

        if table_name in self._mask_columns:
            row_data = self._mask_rows(table_name, row_data)

        full_table_name = f"{self._dst_schema}.{table_name}"
        # Divide rows in chunks of size WRITE_BATCH_SIZE
        chunks = [row_data[i * self.WRITE_BATCH_SIZE:i * self.WRITE_BATCH_SIZE + self.WRITE_BATCH_SIZE]
                  for i in range(ceil(len(row_data) / self.WRITE_BATCH_SIZE))]

        for chunk in chunks:
            # Input rows are dicts. Make sure values are in column order
            values = [[row[a.lower()] for (a, b) in table_columns] for row in chunk]
            write_rows_to_postgresql(self._dst_connection, full_table_name, values)

    def _get_select_list_for_table_definition(self, table_definition: Tuple[str, List]) -> str:
        """
        Returns the select list to read all columns from table.

        :param table_definition:
        :return:
        """
        return ','.join([self._get_select_expr(column_def) for column_def in table_definition[1]])

    def _get_select_expr(self, column_definition: Tuple[str, str]) -> str:
        """
        Returns the select expression to read column data from the source database in a format that
        is understood by the destination database.
        For most columns this will be the column name, but some Oracle columns need a transformation
        on selection to be understood by Postgres.

        :param column_definition:
        :return:
        """
        column_name, column_type = column_definition

        if column_type == 'GEOMETRY':
            return f'SDO_UTIL.TO_WKTGEOMETRY({column_name}) AS {column_name}'
        if column_type == 'BYTEA':
            return f'DBMS_LOB.SUBSTR({column_name}) AS {column_name}'
        # No transform function needed. Just select the column by name
        return column_name

    def clone(self):
        """
        Entry method. Copies the source Oracle database to the destination Postgres database.

        :return:
        """
        self._prepare_destination_database()
        logger.info(f"Start copying data from {self._src_schema} to {self._dst_schema}")
        rows_copied = self._copy_data()
        logger.info(f"Done copying {rows_copied} rows from {self._src_schema} to {self._dst_schema}.")
        return rows_copied
