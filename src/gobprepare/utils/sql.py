"""SQL constants."""


from collections.abc import Iterable

from gobprepare.utils.typing import MetaField

SQL_TYPE_CONVERSIONS = {
    "String": "character varying",
    "DateTime": "timestamp without time zone",
    "Int": "integer",
    "Boolean": "boolean",
    "GeoJSON": "geometry",
}
SQL_QUOTATION_MARK = "'"


def _quote(name: str) -> str:
    """Quote all SQL identifiers (schema, table, column names).

    To prevent weird errors with SQL keywords accidentally being used in identifiers.

    Note that quotation marks may differ per database type.
    Current escape char works for PostgreSQL

    :param name:
    :return:
    """
    QUOTE_CHAR = '"'
    return f"{QUOTE_CHAR}{name}{QUOTE_CHAR}"


def _create_field(name: str, type: str, description: str) -> dict[str, str]:
    """Create a database field.

    :param name:
    :param type:
    :param description:
    :return: dict containing database field properties
    """
    return {"name": _quote(name), "type": SQL_TYPE_CONVERSIONS[type], "description": description}


def get_create_table_sql(
    schema: str,
    table_name: str,
    description: str,
    meta_fields: dict[str, MetaField],
    field_names: Iterable[str],
) -> str:
    """Return a SQL statement to create a table in a schema.

    The table fields are constructed from GraphQL meta fields

    :param table_name:
    :param meta_fields:
    :param fields:
    :return:
    """
    fields = []
    for field_name in field_names:
        field = meta_fields[field_name]
        fields.append(_create_field(field_name, field["type"]["name"], field["description"]))

    table_name = get_full_table_name(schema, table_name)
    table_fields = ",\n  ".join([f"{field['name']} {field['type']}" for field in fields])
    comments = ";\n".join(
        [
            f"COMMENT ON COLUMN {table_name}.{field['name']} "
            f"IS {SQL_QUOTATION_MARK}{field['description']}{SQL_QUOTATION_MARK}"
            for field in fields
        ]
    )

    return f"""
DROP TABLE IF EXISTS {table_name} CASCADE;
-- TRUNCATE TABLE {table_name};
CREATE TABLE IF NOT EXISTS {table_name}
(
  {table_fields}
);
COMMIT;

-- Table and Column comments
COMMENT ON TABLE  {table_name} IS {SQL_QUOTATION_MARK}{description}{SQL_QUOTATION_MARK};
{comments}
"""


def get_full_table_name(schema: str, table_name: str) -> str:
    """Return full table name."""
    return f"{_quote(schema)}.{_quote(table_name)}"
