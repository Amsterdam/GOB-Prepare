import json

from gobprepare.utils.requests import post_stream


class GraphQL:

    def __init__(self, host, endpoint, query):
        self.host = host
        self.endpoint = endpoint
        self.query = query

    @staticmethod
    def item_to_list(item):
        # Convert to list
        items = item if isinstance(item, list) else [item]
        yield from items

    def __iter__(self):
        items = post_stream(f'{self.host}{self.endpoint}', {'query': self.query})
        for item in items:
            yield from self.item_to_list(json.loads(item))
