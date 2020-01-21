from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobprepare.utils.graphql import GraphQL, GraphQLStreaming, GOBException
from typing import Generator


class TestGraphQL(TestCase):

    def test_item_to_list(self):
        graphql = GraphQL('the_host', 'the_url', 'query')
        self.assertEqual(next(graphql.item_to_list('a')), 'a')
        self.assertEqual(next(graphql.item_to_list(['a', 'b'])), 'a')

    @patch("gobprepare.utils.graphql.json.loads", lambda x: 'jl_' + x)
    def test_iter(self):
        graphql = GraphQL('the_host', 'the_url', 'query')
        graphql._query_items = MagicMock(return_value=['a', 'b', 'c', 'd', 'e', 'f'])

        expected_result = ['jl_' + item for item in graphql._query_items.return_value]

        result = [item for item in graphql]
        self.assertEqual(expected_result, result)
        self.assertIsInstance(graphql.__iter__(), Generator)


class TestGraphQLStreaming(TestCase):

    @patch("gobprepare.utils.graphql.json.loads", lambda x: 'jl_' + x)
    def test_iter(self):
        graphql = GraphQLStreaming('the_host', 'the_url', 'query')
        graphql._query_items = MagicMock(return_value=['a', 'b', 'c', 'd', 'e', 'f', b''])

        expected_result = ['jl_' + item for item in graphql._query_items.return_value[:-1]]
        self.assertIsInstance(graphql.__iter__(), Generator)

        result = [item for item in graphql]
        self.assertEqual(expected_result, result)

    @patch("gobprepare.utils.graphql.json.loads", lambda x: 'jl_' + x)
    def test_iter_incomplete_response(self):
        graphql = GraphQLStreaming('the_host', 'the_url', 'query')
        # Missing empty line in result, should raise Exception
        graphql._query_items = MagicMock(return_value=['a', 'b', 'c', 'd', 'e', 'f'])

        with self.assertRaises(GOBException):
            list(graphql)

    @patch("gobprepare.utils.graphql.post_stream")
    @patch("gobprepare.utils.graphql.json.loads", lambda x: 'jl_' + x)
    def test_query(self, mock_post_stream):
        graphql = GraphQLStreaming('the_host', 'the_url', 'query')
        mock_post_stream.return_value = ['a', 'b', b'']
        result = [i for i in graphql]

        url = 'the_host' + 'the_url'
        mock_post_stream.assert_called_with(url, {'query': 'query'})
