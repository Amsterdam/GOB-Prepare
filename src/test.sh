#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo() {
   builtin echo -e "$@"
}

export COVERAGE_FILE="/tmp/.coverage"


echo "Running mypy"
mypy gobprepare

echo "\nRunning unit tests"
coverage run --source=gobprepare -m pytest

echo "\nCoverage report"
coverage report --fail-under=100

echo "\nCheck if Black finds potential reformat fixes"
black --check --diff gobprepare

echo "\nCheck for potential import sort"
isort --check --diff --src-path=gobprepare gobprepare

echo "\nRunning Flake8 style checks"
flake8 gobprepare

echo "\nChecks complete"
