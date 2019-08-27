# GOB-Prepare

Prepare source data before the import is done by the GOB-Import service.

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra)
is required to run this component.

# Docker

## Requirements

* docker-compose >= 1.17
* docker ce >= 18.03

## Run

```bash
docker-compose build
docker-compose up &
```

## Tests

```bash
docker-compose -f src/.jenkins/test/docker-compose.yml build
docker-compose -f src/.jenkins/test/docker-compose.yml run test
```

# Local

## Requirements

* python >= 3.6

## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```

Or activate the previously created virtual environment

```bash
source venv/bin/activate
```

# Run

Optional: Set environment if GOB-Import should connect to secure data sources:

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

## Trigger prepare

Prepares are triggered by the GOB-Workflow module. See the GOB-Workflow README for more details
