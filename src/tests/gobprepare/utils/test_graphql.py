from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobprepare.utils.graphql import GraphQL
from typing import Generator


class TestGraphQL(TestCase):

    @patch("gobprepare.utils.graphql.post_stream")
    @patch("gobprepare.utils.graphql.json.loads", lambda x: 'jl_' + x)
    def test_iter(self, mock_post_stream):
        graphql = GraphQL('the_host', 'the_url', 'query')

        mock_post_stream.return_value = ['a', 'b', 'c', 'd', 'e', 'f']

        expected_result = ['jl_' + item for item in mock_post_stream.return_value]
        self.assertIsInstance(graphql.__iter__(), Generator)

        result = [item for item in graphql]
        self.assertEqual(expected_result, result)

    @patch("gobprepare.utils.graphql.post_stream")
    @patch("gobprepare.utils.graphql.json.loads", lambda x: 'jl_' + x)
    def test_query(self, mock_post_stream):
        graphql = GraphQL('the_host', 'the_url', 'query')
        mock_post_stream.return_value = ['a', 'b']
        result = [i for i in graphql]

        url = 'the_host' + 'the_url'
        mock_post_stream.assert_called_with(url, {'query': 'query'})

    def test_item_to_list(self):
        graphql = GraphQL('the_host', 'the_url', 'query')
        self.assertEqual(next(graphql.item_to_list('a')), 'a')
        self.assertEqual(next(graphql.item_to_list(['a', 'b'])), 'a')
