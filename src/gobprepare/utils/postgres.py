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


def brp_build_date_json(query: str) -> str:
    """Convert a date string to a date json in sql query.

    input: sql query with some datum statement as "brp_datum_prefix_tb."Datum"     AS datum"
    output: sql query with the datum statement as date json
    prefix "brp_datum_prefix_" used to identify datum columns in de sql query.
    """
    statements = query.splitlines()
    new_query = ""
    for line in statements:
        if "brp_datum_prefix_" in line:
            brp_datum = line.split("brp_datum_prefix_")[1].split(" AS ")
            src_column_name = brp_datum[0].strip()
            column_name = brp_datum[1].strip()

            date_statement = f"""
                CASE
                    WHEN {src_column_name} IS NULL THEN NULL
                    ELSE JSONB_BUILD_OBJECT(
                        'datum', CONCAT_WS(
                            '-',
                            substring({src_column_name}, 1, 4),
                            substring({src_column_name}, 5, 2),
                            substring({src_column_name}, 7, 2)
                        ),
                        'jaar', substring({src_column_name}, 1, 4),
                        'maand', substring({src_column_name}, 5, 2),
                        'dag', substring({src_column_name}, 7, 2)
                        )
                END                                                                  AS {column_name}"""
            line = date_statement
        new_query = f"""{new_query}{line}\n"""
    return new_query
