import itertools
from os import PathLike
from typing import Iterator, Optional, cast

from gobcore.datastore.datastore import Datastore
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger

from gobprepare.selector.typing import SelectorConfig


class Selector:
    """Base Selector.

    Selector handles execution of queries on src_connection. The results of the queries are written to
    """

    WRITE_BATCH_SIZE = 25000

    def __init__(self, src_datastore: Datastore, dst_datastore: Datastore, config: SelectorConfig) -> None:
        """Initialise Selector.

        :param src_datastore:
        :param dst_datastore:
        :param config:
        """
        self._src_datastore = src_datastore
        self._dst_datastore = dst_datastore
        self._config = config
        self.destination_table = config["destination_table"]
        self.ignore_missing = config.get("ignore_missing", False)
        self.query = self._get_query(config)

    def _get_query(self, config: SelectorConfig) -> str:
        src = config["query_src"]

        if src == "string":
            if isinstance(config["query"], list):
                return "\n".join(config["query"])
            return cast(str, config["query"])
        elif src == "file":
            with open(cast(PathLike[str], config["query"])) as f:
                return f.read()

        raise NotImplementedError

    def select(self) -> int:
        """Create destination table if desired and saves result of select query in destination table.

        Entry method.

        :param query:
        :param destination_table:
        :return:
        """
        if self.destination_table.get("create", False):
            # See ToPostgresSelector._create_destination_table
            self._create_destination_table(self.destination_table)  # type: ignore[attr-defined]

        total_cnt = 0
        # See FromDatastoreSelector._read_rows
        rows: Iterator[dict[str, str]] = self._read_rows(self.query)  # type: ignore[attr-defined]

        columns: list[dict[str, str]] = self.destination_table["columns"]
        name = self.destination_table["name"]

        while True:
            chunk = itertools.islice(rows, self.WRITE_BATCH_SIZE)
            values = self._values_list(chunk, columns)
            # See ToPostgresSelector._write_rows
            result_rows = self._write_rows(name, values)  # type: ignore[attr-defined]

            total_cnt += result_rows

            if result_rows < self.WRITE_BATCH_SIZE:
                self._dst_datastore.execute(f"ANALYZE {name}")
                logger.info(f"Written {total_cnt:,} rows to destination table {name}")
                return total_cnt

    def _values_list(self, rows: Iterator[dict[str, str]], columns: list[dict[str, str]]) -> list[list[str]]:
        """Transform the rows to lists of values in the order as specified by columns.

        If a column:value pair is missing for a column present in columns, a GOBException is raised when
        self.ignore_missing == False. If self.ignore_missing == True, the value for that column will be set to None.

        :param rows: dictionaries of column:value pairs
        :param columns:
        :return:
        """
        ignore_missing = self.ignore_missing

        def match_column(column: dict[str, str], row: dict[str, str]) -> Optional[str]:
            name = column["name"].lower()

            if name in row or ignore_missing:
                return row.get(name, None)
            else:
                raise GOBException(f"Missing column {name} in query result")

        def prepare_row(row: dict[str, str]) -> list[str]:
            row_values = [match_column(column, row) for column in columns]
            # See ToPostgresSelector._prepare_row
            return self._prepare_row(row_values, columns)  # type: ignore[attr-defined, no-any-return]

        return [prepare_row(row) for row in rows]
