import json

from gobprepare.utils.requests import post_stream


class GraphQL:

    def __init__(self, host, endpoint, query):
        self.host = host
        self.endpoint = endpoint
        self.query = query

    @staticmethod
    def item_to_list(item):
        # Convert to list if it is not a list
        item = item if isinstance(item, list) else [item]
        yield from item

    def __iter__(self):
        """Post to GraphQL API and yield items from the stream.

        GraphQL output for looks like this:
        [b'{"node": {"a": "1", "b": "xx"}}', b'{"node": {"a": "2", "b": "yy"}}']

        Convert each item to JSON and yield it.
        The output looks like this:
        {"node": {"a": "1", "b": "xx"}}
        {"node": {"a": "2", "b": "yy"}}
        """
        items = post_stream(f'{self.host}{self.endpoint}', {'query': self.query})
        for item in items:
            yield from self.item_to_list(json.loads(item))
