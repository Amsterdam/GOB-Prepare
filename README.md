# GOB-Prepare

Prepare source data before the import is done by the [GOB-Import](https://github.com/Amsterdam/GOB-Import) service.

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra)
is required to run this component.

# Prerequisites

Prepare should run after Import BAG is completed.
This is because BAG verblijfsobjecten are required for `import_verblijfsobjecten_geometrie` action.

# Docker

## Requirements

* docker compose >= 1.25
* Docker CE >= 18.09

## Run

```bash
docker compose build
docker compose up -d
```

## Tests

```bash
docker compose -f src/.jenkins/test/docker-compose.yml build && docker compose -f src/.jenkins/test/docker-compose.yml run --rm test
```

# Local

## Requirements

* Python >= 3.9

## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```

Or activate the previously created virtual environment:

```bash
source venv/bin/activate
```

# Run

Optional: Set environment if GOB-Prepare should connect to secure data sources:

```bash
export $(cat .env | xargs)  # Copy from .env.example if missing
```

Start the service:

```bash
cd src
python -m gobprepare
```

## Tests

Run the tests:

```bash
cd src
sh test.sh
```

# Remarks

## Trigger Prepare

Prepares are triggered by the [GOB-Workflow](https://github.com/Amsterdam/GOB-Workflow) module. See the GOB-Workflow README for more details.
