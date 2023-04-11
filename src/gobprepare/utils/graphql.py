import json

from gobcore.exceptions import GOBException

from gobprepare.utils.requests import post_stream


class GraphQL:
    """GraphQL API class."""

    def __init__(self, host, endpoint, query):
        """Initialise GraphQL."""
        self.host = host
        self.endpoint = endpoint
        self.query = query

    @staticmethod
    def item_to_list(item):
        """Convert item to list if it is not a list."""
        item = item if isinstance(item, list) else [item]
        yield from item

    def _query_items(self):
        return post_stream(f"{self.host}{self.endpoint}", {"query": self.query})

    def __iter__(self):
        """Post to GraphQL API and yield items from the stream.

        GraphQL output for looks like this:
        [b'{"node": {"a": "1", "b": "xx"}}', b'{"node": {"a": "2", "b": "yy"}}']

        Convert each item to JSON and yield it.
        The output looks like this:
        {"node": {"a": "1", "b": "xx"}}
        {"node": {"a": "2", "b": "yy"}}
        """
        for item in self._query_items():
            yield from self.item_to_list(json.loads(item))


class GraphQLStreaming(GraphQL):
    """GraphQLStreaming works as the GraphQL class, but verifies that the response received is complete."""

    def __iter__(self):
        """Verify that the response received is complete."""
        last_item = None
        for item in self._query_items():
            last_item = item

            if item != b"":
                yield from self.item_to_list(json.loads(item))

        if last_item != b"":
            raise GOBException("Received incomplete response from GOB API")
