from typing import Iterator


class FromDatastoreSelector:
    """From Generic Datastore Selector."""

    BATCH_SIZE = 2_000

    def _read_rows(self, query: str) -> Iterator[dict[str, str]]:
        """Read rows from datastore.

        # kwargs apply to psycopg2 cursor
        # use named cursor to use server-side-cursor and allow to be used again
        # arraysize is batchsize of query results
        """
        yield from self._src_datastore.query(  # type: ignore[attr-defined]
            query, name="named_cursor", arraysize=self.BATCH_SIZE, withhold=True
        )
