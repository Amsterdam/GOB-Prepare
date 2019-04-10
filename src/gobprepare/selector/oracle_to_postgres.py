"""
OracleToPostgresSelector class

Contains logic to run a (select) query on an oracle database and import this in a new table in Postgres.
"""
from gobprepare.selector._selector import Selector
from gobprepare.selector._from_oracle import FromOracleSelector
from gobprepare.selector._to_postgres import ToPostgresSelector


class OracleToPostgresSelector(Selector, FromOracleSelector, ToPostgresSelector):
    pass
