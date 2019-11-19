import os
import re

from gobcore.exceptions import GOBException
from sqlalchemy.engine.url import URL

ORACLE_DRIVER = 'oracle+cx_oracle'
POSTGRES_DRIVER = 'postgresql'
DEBUG = os.getenv('GOB_DEBUG', False)

STREAMING_GRAPHQL_ENDPOINT = '/gob/graphql/streaming/'
GRAPHQL_ENDPOINT = '/gob/graphql/'
WRITE_BATCH_SIZE = 10000
GOB_API_HOST = os.getenv('API_HOST', 'http://localhost:8141')

CONTAINER_BASE = os.getenv("CONTAINER_BASE", "acceptatie")

DATABASE_CONFIGS = {
    'Neuron': {
        'drivername': ORACLE_DRIVER,
        'username': os.getenv("BINNRN_DATABASE_USER", "gob"),
        'password': os.getenv("BINNRN_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("BINNRN_DATABASE_HOST", "hostname"),
        'port': os.getenv("BINNRN_DATABASE_PORT", 1521),
        'database': os.getenv("BINNRN_DATABASE", ""),
    },
    'GOBPrepare': {
        'drivername': POSTGRES_DRIVER,
        'username': os.getenv("GOB_PREPARE_DATABASE_USER", "gob"),
        'password': os.getenv("GOB_PREPARE_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("PREPARE_DATABASE_HOST_OVERRIDE", os.getenv("GOB_PREPARE_DATABASE_HOST", "hostname")),
        'port': os.getenv("PREPARE_DATABASE_PORT_OVERRIDE", os.getenv("GOB_PREPARE_DATABASE_PORT", 5408)),
        'database': os.getenv("GOB_PREPARE_DATABASE", ""),
    }
}

OBJECTSTORE_CONFIGS = {
    'Basisinformatie': {
        "VERSION": '2.0',
        "AUTHURL": 'https://identity.stack.cloudvps.com/v2.0',
        "TENANT_NAME": os.getenv("BASISINFORMATIE_OBJECTSTORE_TENANT_NAME"),
        "TENANT_ID": os.getenv("BASISINFORMATIE_OBJECTSTORE_TENANT_ID"),
        "USER": os.getenv("BASISINFORMATIE_OBJECTSTORE_USER"),
        "PASSWORD": os.getenv("BASISINFORMATIE_OBJECTSTORE_PASSWORD"),
        "REGION_NAME": 'NL'
    }
}


def get_objectstore_config(name: str):
    try:
        config = OBJECTSTORE_CONFIGS[name]
    except KeyError:
        raise GOBException(f"Objectstore config for source {name} not found.")

    config['name'] = name
    return config


def get_database_config(name: str):
    try:
        config = DATABASE_CONFIGS[name].copy()
    except KeyError:
        raise GOBException(f"Database config for source {name} not found.")

    config['url'] = get_url(config)
    config['name'] = name
    return config


def get_url(db_config):
    """Get URL connection

    Get the URL for the given database config

    :param db_config: e.g. DATABASE_CONFIGS['DIVA']
    :return: url
    """
    # Default behaviour is to return the sqlalchemy url result
    url = URL(**db_config)
    if db_config["drivername"] == ORACLE_DRIVER:
        # The Oracle driver can accept a service name instead of a SID
        service_name_pattern = re.compile("^\w+\.\w+\.\w+$")
        if service_name_pattern.match(db_config["database"]):
            # Replace the SID by the service name
            url = str(url).replace(db_config["database"], '?service_name=' + db_config['database'])
    return url
