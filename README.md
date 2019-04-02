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

# NB This is the only prepare job currently available. Takes a long time!
docker exec gobworkflow python -m gobworkflow.start prepare data/brk.prepare.json
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

Start the service:

```bash
export $(cat .env | xargs)  # Copy from .env.example if missing
cd src
python -m gobprepare
```

Start a single import in another window:

```bash
cd ../GOB/GOB-Workflow/src
# Start a single import (requires VPN connection)
# NB This is the only prepare job currently available. Takes a long time!
python -m gobworkflow.start prepare data/brk.prepare.json
```


## Tests

Run the tests:

```bash
cd src
sh test.sh
```
