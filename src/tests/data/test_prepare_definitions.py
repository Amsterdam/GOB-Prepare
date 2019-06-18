import json
import os

from unittest import TestCase
from unittest.mock import patch

from gobprepare.prepare_client import PrepareClient


@patch('gobprepare.prepare_client.logger')
class TestPrepareDefinitions(TestCase):
    files = [
        # Relative to 'src' directory
        'data/brk.prepare.json'
    ]

    def _get_filepath(self, filename):
        return os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', filename)

    def _test_prepare_definition(self, definition: dict):
        actions = definition.get('actions', [])
        self.assertTrue(len(actions) > 0, "No actions defined")

        self._validate_unique_ids(actions)
        self._validate_actions_dependencies(actions)
        self._validate_file_references_exist(definition)

    def _validate_file_references_exist(self, definition: dict):

        for action in definition.get('actions', []):
            if action.get('query_src') == 'file':
                fileref = action.get('query')
                self.assertIsNotNone(fileref, f"Query src for action '{action['id']}' is 'file', but no file supplied")

                with open(self._get_filepath(fileref)) as f:
                    pass

    def _validate_unique_ids(self, actions):
        ids = [action['id'] for action in actions]
        self.assertTrue(len(set(ids)) == len(actions))

    def _validate_actions_dependencies(self, actions):
        actions_done = []

        for action in actions:
            if "depends_on" in action:
                for depends_on in action["depends_on"]:
                    self.assertTrue(depends_on in actions_done)

            actions_done.append(action["id"])

    def test_prepare_definitions(self, mock_logger):
        # Test if config loads correctly and definition dependencies are in order
        for file in self.files:
            with open(self._get_filepath(file)) as f:
                definition = json.load(f)
                self._test_prepare_definition(definition)



