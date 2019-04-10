from gobprepare.selector._selector import Selector
from gobprepare.selector._from_postgres import FromPostgresSelector
from gobprepare.selector._to_postgres import ToPostgresSelector


class PostgresToPostgresSelector(Selector, FromPostgresSelector, ToPostgresSelector):
    pass
