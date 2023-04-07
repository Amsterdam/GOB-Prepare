"""Mapping.

Read a mapping from a file.
"""


import json
import os

from gobcore.exceptions import GOBException

DATASET_DIR = os.path.join(os.path.dirname(__file__), "../data/")


def _build_prepare_definitions_locations_mapping():
    """Build prepare definitions locations mapping based on JSON files present in DATASET_DIR.

    :return:
    """
    result = {}

    for file in os.listdir(DATASET_DIR):
        filepath = DATASET_DIR + file
        if os.path.isfile(filepath) and file.endswith(".json") and not file.startswith("_"):
            try:
                mapping = get_mapping(filepath)
                catalogue = mapping["catalogue"]
            except (KeyError, json.decoder.JSONDecodeError):
                raise GOBException(f"Dataset file {filepath} invalid")
            result[catalogue] = filepath
    return result


def get_prepare_definition_file_location(catalogue: str):
    """Return the catalog prepare definitions mapping."""
    try:
        return prepare_definition_locations_mapping[catalogue]
    except KeyError:
        raise GOBException(f"No prepare definition found for catalogue {catalogue}")


def get_mapping(input_name):
    """Read a mapping from a file.

    :param input_name: name of the file that contains the mapping
    :return: an object that contains the mapping
    """
    with open(input_name) as file:
        return json.load(file)


prepare_definition_locations_mapping = _build_prepare_definitions_locations_mapping()
