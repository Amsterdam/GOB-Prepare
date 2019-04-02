import json

from unittest import TestCase
from unittest.mock import patch, mock_open, MagicMock

from gobprepare.mapping import get_mapping


class TestMapping(TestCase):
    expected_output = {
        'key_a': 'val_a',
        'key_b': 'val_b',
    }

    @patch("builtins.open", new_callable=mock_open, read_data=json.dumps(expected_output))
    def test_get_mapping(self, mock_file):
        filename = "somefile.csv"
        result = get_mapping(filename)
        mock_file.assert_called_with(filename)
        self.assertEqual(result, self.expected_output)
