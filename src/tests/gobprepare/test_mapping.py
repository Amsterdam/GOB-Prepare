import json

from unittest import TestCase
from unittest.mock import patch, mock_open, MagicMock

from gobcore.exceptions import GOBException
from gobprepare.mapping import get_mapping, _build_prepare_definitions_locations_mapping, get_prepare_definition_file_location


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

    @patch("gobprepare.mapping.get_mapping")
    @patch("gobprepare.mapping.os")
    @patch("gobprepare.mapping.DATASET_DIR", "mocked/data/dir/")
    def test_build_prepare_definitions_locations_mapping(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.return_value = {
            'catalogue': 'somecatalogue',
        }

        expected_result = {
            'somecatalogue': 'mocked/data/dir/file.json'
        }

        result = _build_prepare_definitions_locations_mapping()
        self.assertEqual(expected_result, result)

    @patch("gobprepare.mapping.get_mapping")
    @patch("gobprepare.mapping.os")
    @patch("gobprepare.mapping.DATASET_DIR", "mocked/data/dir/")
    def test_build_prepare_definitions_locations_mapping_invalid_dict(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.return_value = {
        }

        with self.assertRaisesRegexp(GOBException, "Dataset file mocked/data/dir/file.json invalid"):
            _build_prepare_definitions_locations_mapping()

    @patch("gobprepare.mapping.get_mapping")
    @patch("gobprepare.mapping.os")
    @patch("gobprepare.mapping.DATASET_DIR", "mocked/data/dir/")
    def test_build_prepare_definitions_locations_mapping_invalid_json(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.side_effect = json.decoder.JSONDecodeError("", MagicMock(), 0)

        with self.assertRaisesRegexp(GOBException, "Dataset file mocked/data/dir/file.json invalid"):
            _build_prepare_definitions_locations_mapping()

    mock_mapping = {
        'cat_a': 'somefile.json'
    }

    @patch("gobprepare.mapping.prepare_definition_locations_mapping", mock_mapping)
    def test_get_prepare_definition_file_location(self):
        self.assertEqual('somefile.json', get_prepare_definition_file_location('cat_a'))

        with self.assertRaisesRegexp(GOBException, "No prepare definition found for catalogue cat_b"):
            get_prepare_definition_file_location('cat_b')
