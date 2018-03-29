# Snuba

A service providing fast event searching, filtering and aggregation on arbitrary fields.

## Requirements

Snuba assumes a Clickhouse server endpoint at `CLICKHOUSE_SERVER` (default `localhost:9000`).

## Install / Run

    mkvirtualenv snuba

    # Run API server
    ./bin/api

## API

Snuba exposes an HTTP API with the following endpoints.

- [/](/): Shows this page.
- [/query](/query): GET endpoint for querying clickhouse.

## Settings

Settings are found in `settings.py`

- `CLICKHOUSE_SERVER` : The endpoint for the clickhouse service.
- `CLICKHOUSE_TABLE` : The clickhouse table name.

## Tests

    docker run -d -p 9000:9000 -p 9009:9009 -p 8123:8123 \
      --name clickhouse-server --ulimit nofile=262144:262144 yandex/clickhouse-server

    pip install -r requirements.txt
    python setup.py develop

    pytest
