import os
DEBUG = os.getenv('GOB_DEBUG', False)

STREAMING_GRAPHQL_ENDPOINT = '/gob/public/graphql/streaming/'
GRAPHQL_ENDPOINT = '/gob/public/graphql/'
WRITE_BATCH_SIZE = 10000
GOB_API_HOST = os.getenv('API_HOST', 'http://localhost:8141')

CONTAINER_BASE = os.getenv("CONTAINER_BASE", "acceptatie")
