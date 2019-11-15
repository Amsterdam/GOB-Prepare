from unittest import TestCase
from unittest.mock import MagicMock, patch

from requests.exceptions import RequestException
from gobprepare.utils.requests import APIException, post_stream


class MockResponse:

    def __init__(self, exc):
        self.exc = exc

    def raise_for_status(self):
        raise self.exc("Any reason")


class MockGet:
    status_code = 123

    def raise_for_status(self):
        pass


class TestRequests(TestCase):

    def setUp(self):
        pass

    @patch("gobprepare.utils.requests.requests")
    def test_post_stream(self, mock_requests):
        result = post_stream('url', 'some json')
        mock_requests.post.assert_called_with('url', stream=True, json='some json')
        self.assertEqual(mock_requests.post.return_value.iter_lines.return_value, result)

    @patch("gobprepare.utils.requests.requests")
    def test_post_stream_params(self, mock_requests):
        kwargs = {'abc': 'def', 'ghi': 'jkl'}
        result = post_stream('url', 'some json', **kwargs)
        mock_requests.post.assert_called_with('url', stream=True, json='some json', **kwargs)

    @patch("gobprepare.utils.requests.requests.post")
    def test_post_stream_exception(self, mock_requests_post):
        mock_get = MockGet()
        mock_get.raise_for_status = MagicMock(side_effect=RequestException)
        mock_requests_post.return_value = mock_get

        with self.assertRaisesRegexp(APIException, 'Request failed due to API exception'):
            post_stream('any url', True)
