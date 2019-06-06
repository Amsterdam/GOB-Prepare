import json
import os

from unittest import TestCase

from gobprepare.prepare_client import PrepareClient


class TestPrepareDefinitions(TestCase):
    files = [
        # Relative to 'src' directory
        'data/brk.prepare.json'
    ]

    def _get_filepath(self, filename):
        return os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', filename)

    def _test_prepare_definition(self, definition: dict):
        prepare_client = PrepareClient(definition, {})
        prepare_client._validate_actions_dependencies()

        self.assertTrue(len(definition.get('actions', [])) > 0, "No actions defined")
        self._validate_file_references_exist(definition)

    def _validate_file_references_exist(self, definition: dict):

        for action in definition.get('actions', []):
            if action.get('query_src') == 'file':
                fileref = action.get('query')
                self.assertIsNotNone(fileref, f"Query src for action '{action['id']}' is 'file', but no file supplied")

                with open(self._get_filepath(fileref)) as f:
                    pass

    def test_prepare_definitions(self):
        # Test if config loads correctly and definition dependencies are in order
        for file in self.files:
            with open(self._get_filepath(file)) as f:
                definition = json.load(f)
                self._test_prepare_definition(definition)



