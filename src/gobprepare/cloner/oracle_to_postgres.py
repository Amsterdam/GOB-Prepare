"""
Contains the OracleToPostgresCloner class, which contains the logic to clone an Oracle database schema to a Postgres
database.
"""
import time
import re

from math import ceil
from typing import Dict, List, Tuple

from gobprepare.config import DEBUG
from gobcore.exceptions import GOBException, GOBEmptyResultException
from gobcore.logging.logger import logger
from gobcore.database.reader.oracle import read_from_oracle
from gobprepare.cloner.mapping.oracle_to_postgres import \
    get_postgres_column_definition
from gobcore.database.writer.postgresql import (drop_table, execute_postgresql_query,
                                                write_rows_to_postgresql)


class OracleToPostgresCloner():
    READ_BATCH_SIZE = 100000
    WRITE_BATCH_SIZE = 100000

    schema_definition = None
    _ignore_tables = []
    _include_tables = []
    _id_columns = {}

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
            self._ignore_tables = config.get('ignore', [])
            self._include_tables = config.get('include', [])
            self._id_columns = config.get('id_columns', {})

    def _filter_tables(self, tables: list):
        def matches_any(table_name: str, patterns: list):
            """Returns True if table_name matches any pattern in list

            :param table_name:
            :return:
            """
            return any([pattern.match(table_name) for pattern in patterns])

        if self._ignore_tables:
            patterns = [re.compile(pattern) for pattern in self._ignore_tables]

            return [table for table in tables if not matches_any(table['table_name'], patterns)]
        elif self._include_tables:
            patterns = [re.compile(pattern) for pattern in self._include_tables]

            return [table for table in tables if matches_any(table['table_name'], patterns)]
        else:
            return tables

    def read_source_table_names(self) -> list:
        """Returns a list of table names present in given schema

        :return:
        """
        query = f"SELECT table_name FROM all_tables WHERE owner='{self._src_schema}' ORDER BY table_name"
        table_names = read_from_oracle(self._src_connection, [query])
        table_names = self._filter_tables(table_names)

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
            f"FROM all_tab_columns WHERE owner='{self._src_schema}' AND table_name='{table}' ORDER BY column_id"
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
            table_names = self.read_source_table_names()
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

    def _list_to_chunks(self, lst: list, chunk_size: int):
        """Divides a list in chunks of chunk_size. Returns a list of dictionaries with "min" and "max" as keys, where
        "min" is inclusive, and "max" is exclusive.

        :param lst:
        :param chunk_size:
        :return:
        """
        assert chunk_size > 0

        result = []
        min_idx = 0

        while min_idx < len(lst):
            max_idx = min_idx + chunk_size
            result.append({
                'min': lst[min_idx] if min_idx > 0 else None,
                'max': lst[max_idx] if max_idx < len(lst) else None,
            })
            min_idx = max_idx

        return result

    def _get_id_columns_for_table(self, table_name: str):
        """Returns the list of id columns for table (as defined in the prepare definition)

        :param table_name:
        :return:
        """
        table_name = table_name.split('.')[-1]

        if table_name in self._id_columns:
            return self._id_columns[table_name]
        elif "_default" in self._id_columns:
            return self._id_columns["_default"]
        else:
            raise GOBException(f"Missing id columns for table {table_name}")

    def _get_ids_for_table(self, full_table_name: str) -> list:
        """Returns a list of the id's present in the source table

        :param full_table_name:
        :return:
        """
        order_field = self._get_id_columns_for_table(full_table_name)[0]
        query = f"SELECT {order_field} FROM {full_table_name} ORDER BY {order_field}"

        try:
            result = read_from_oracle(self._src_connection, [query])
        except GOBEmptyResultException:
            result = []

        return [row[order_field.lower()] for row in result]

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

        ids = self._get_ids_for_table(full_table_name)
        row_cnt = 0

        chunks = self._list_to_chunks(ids, self.READ_BATCH_SIZE)
        order_field = self._get_id_columns_for_table(full_table_name)[0]

        for chunk in chunks:
            if chunk['min'] or chunk['max']:
                where_max = f"{order_field} < '{chunk['max']}'" if chunk['max'] is not None else ""
                where_min = f"{order_field} >= '{chunk['min']}'" if chunk['min'] is not None else ""
                where = f"WHERE {where_min} {'AND' if where_min and where_max else ''} {where_max}"
            else:
                where = ""
            query = "" \
                f"SELECT /*+ PARALLEL */ {outer_select} FROM (" \
                f"  SELECT {inner_select} FROM {full_table_name} {where}" \
                f")"

            read_time = time.time()
            try:
                results = read_from_oracle(self._src_connection, [query])
            except GOBEmptyResultException:
                results = []
            read_time = time.time() - read_time

            row_cnt += len(results)

            write_start = time.time()
            self._insert_rows(table_definition, results)
            write_time = time.time() - write_start

            if DEBUG:
                logger.info("Total: {:>8.1f}s  "
                            "Read: {:>8.1f}s  "
                            "Write: {:>8.1f}s  "
                            "This: {:>8} rows  "
                            "Total: {:>8} rows  ({})".format(
                                read_time + write_time,
                                read_time,
                                write_time,
                                len(results),
                                row_cnt,
                                table_name
                            )
                            )
        # We're done
        logger.info(f"Written {row_cnt} rows to destination table {full_table_name}")
        return row_cnt

    def _insert_rows(self, table_definition: Tuple[str, List], row_data: List[Dict]) -> None:
        """
        Inserts row_data into table. Input is a list of dicts with column: value pairs.

        Data is inserted in chunks of WRITE_BATCH_SIZE.

        :param table_definition:
        :param row_data:
        :return:
        """
        table_name, table_columns = table_definition

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
