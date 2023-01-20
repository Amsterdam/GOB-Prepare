"""Helper function for translating an Oracle column type to Postgres

"""

from gobcore.exceptions import GOBException


def _oracle_number_to_postgres(length: int = None, precision: int = None, scale: int = None) -> str:
    """
    Returns the Postgres equivalent of the Oracle NUMBER column type for given length, precision and
    scale

    :param length:
    :param precision:
    :param scale:
    :return:
    """
    if precision is not None and scale is not None:
        return f'NUMERIC({precision},{scale})'

    if length is None:
        raise GOBException('Expect Oracle NUMBER to have either precision and scale or length')

    if length <= 4:
        return 'SMALLINT'
    elif length <= 9:
        return 'INT'
    elif length <= 18:
        return 'BIGINT'
    else:
        return 'NUMERIC'


_oracle_postgresql_column_mapping = {
    # functions receive three parameters: length, precision, scale
    'VARCHAR2': lambda length, *_: f'VARCHAR({length})',
    'NARCHAR2': lambda length, *_: f'VARCHAR({length})',
    'CHAR': lambda length, *_: f'CHAR({length})',
    'NCHAR': lambda length, *_: f'CHAR({length})',
    'DATE': lambda *_: 'TIMESTAMP(0)',
    'TIMESTAMP WITH LOCAL TIME ZONE': lambda *_: 'TIMESTAMPTZ',
    'CLOB': lambda *_: 'TEXT',
    'NCLOB': lambda *_: 'TEXT',
    'BLOB': lambda *_: 'BYTEA',
    'NUMBER': _oracle_number_to_postgres,
    'SDO_GEOMETRY': lambda *_: 'GEOMETRY',
}


def get_postgres_column_definition(data_type: str, length: int = None, precision: int = None,
                                   scale: int = None) -> str:
    """
    Returns the (string representation of the) Postgres equivalent of an Oracle column definition.

    For example:
    - given data_type VARCHAR2 and length 2, this function returns 'VARCHAR(2)'.
    - given data_type NUMBER, precision 2 and scale 1, this function returns 'NUMERIC(2,1)'

    :param data_type:
    :param length:
    :param precision:
    :param scale:
    :return:
    """
    try:
        return _oracle_postgresql_column_mapping[data_type](length, precision, scale)
    except KeyError:
        raise GOBException(f"Missing column type definition mapping for {data_type}")
