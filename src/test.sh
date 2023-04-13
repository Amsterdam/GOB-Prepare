#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo() {
   builtin echo -e "$@"
}

export COVERAGE_FILE="/tmp/.coverage"

# Uncomment files to pass through checks
FILES=(
  "gobprepare/cloner/mapping/__init__.py"
  "gobprepare/cloner/mapping/oracle_to_postgres.py"
  "gobprepare/cloner/__init__.py"
  "gobprepare/cloner/oracle_to_postgres.py"
  "gobprepare/cloner/typing.py"
  "gobprepare/selector/_selector.py"
  "gobprepare/selector/__init__.py"
  "gobprepare/selector/_from_datastore.py"
  "gobprepare/selector/_to_postgres.py"
  "gobprepare/selector/datastore_to_postgres.py"
  "gobprepare/selector/typing.py"
  "gobprepare/config.py"
  "gobprepare/importers/__init__.py"
#  "gobprepare/importers/api_importer.py"
#  "gobprepare/importers/csv_importer.py"
  "gobprepare/__init__.py"
  "gobprepare/utils/graphql.py"
  "gobprepare/utils/__init__.py"
  "gobprepare/utils/postgres.py"
  "gobprepare/utils/exceptions.py"
  "gobprepare/utils/sql.py"
  "gobprepare/utils/requests.py"
  "gobprepare/utils/typing.py"
  "gobprepare/mapping.py"
#  "gobprepare/prepare_client.py"
  "gobprepare/typing.py"
#  "gobprepare/__main__.py"
)

echo "Running mypy"
mypy "${FILES[@]}"

echo "\nRunning unit tests"
coverage run --source=gobprepare -m pytest

echo "Coverage report"
coverage report --fail-under=100

echo "\nCheck if Black finds potential reformat fixes"
black --check --diff "${FILES[@]}"

echo "\nCheck for potential import sort"
isort --check --diff --src-path=gobprepare "${FILES[@]}"

echo "\nRunning Flake8 style checks"
flake8 "${FILES[@]}"

echo "\nChecks complete"
