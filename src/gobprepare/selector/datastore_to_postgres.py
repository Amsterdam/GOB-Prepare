"""
DatastoreToPostgresSelector

Contains logic to run a (select) query on an oracle database and import this in a new table in Postgres.
"""
from gobprepare.selector._selector import Selector
from gobprepare.selector._from_datastore import FromDatastoreSelector
from gobprepare.selector._to_postgres import ToPostgresSelector


class DatastoreToPostgresSelector(Selector, FromDatastoreSelector, ToPostgresSelector):
    pass
