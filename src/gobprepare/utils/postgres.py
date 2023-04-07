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
