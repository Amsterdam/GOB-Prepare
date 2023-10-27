from functools import cache

from gobcore.datastore.postgres import PostgresDatastore
from gobcore.logging.logger import logger


@cache
def _is_columnar_supported(postgres_datastore: PostgresDatastore) -> bool:
    result = int(postgres_datastore.get_version().split(".")[0]) >= 12 and postgres_datastore.is_extension_enabled(
        "citus"
    )

    if not result:
        logger.warning(
            "Columnar tables are not supported by this Postgres instance. " "Need version >= 12 with 'citus' enabled."
        )
    return result


def create_table_columnar_as_query(postgres_datastore: PostgresDatastore, tablename: str, as_query: str) -> str:
    """Return the CREATE TABLE query for Postgres for a CREATE TABLE tablename AS as_query query.

    Create a columnar table if supported by this Postgres instance.
    """
    if _is_columnar_supported(postgres_datastore):
        return f"CREATE TABLE {tablename} USING columnar AS {as_query}"
    return f"CREATE TABLE {tablename} AS {as_query}"


def create_table_columnar_query(postgres_datastore: PostgresDatastore, tablename: str, columndefs: str) -> str:
    """Return the CREATE TABLE query for Postgres.

    :columndefs: is the columns definition that goes between the parentheses, without the parentheses

    Example: CREATE TABLE tablename (columndefs)

    Create a columnar table if supported by this Postgres instance.
    """
    if _is_columnar_supported(postgres_datastore):
        return f"CREATE TABLE {tablename} ({columndefs}) USING columnar"
    return f"CREATE TABLE {tablename} ({columndefs})"

def check_table_existence_query(schema: str, table_name: str) -> str:
    """Return table exists query"""
    return f"""SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE schemaname = '{schema}'
                AND tablename  = '{table_name}'
            )"""

def create_update_table_query(table_name: str, columns_values: list[dict[str]], conditions: list[dict[str]]) -> str:
    """Return update table query"""
    condition_list = ""
    update_list = ""
    for key, value in conditions[0].items():
        condition_list = condition_list +  key + " = '" + value + "' AND "

    for key, value in columns_values[0].items():
        #TODO: value moet een string zijn. CURRENT_DATE
        update_list = update_list + key + " = " + value + ","
    query = f"""UPDATE {table_name}
                SET {update_list[:-1]}
                WHERE {condition_list[:-5]}"""
    return query

def create_select_where_query(table_name: str, conditions: list[dict[str]], columns: [str] = '*') -> str:
    """Return select query """

    condition_list = ""
    for key, value in conditions[0].items():
        condition_list = condition_list +  key + " = '" + value + "' AND "
        if columns == "*":
            query = f"""SELECT {columns}
                FROM {table_name}
                WHERE {condition_list[:-5]}"""
        else:
            columns_list = ','.join(str(col) for col in columns)
            query = f"""SELECT {columns_list}
                FROM {table_name}
                WHERE {condition_list[:-5]}"""

    return query 
