# Snuba

A service providing fast event searching, filtering and aggregation on arbitrary fields.

## Requirements

Snuba assumes a Clickhouse server HTTP endpoint at `CLICKHOUSE_SERVER` (default `localhost:8123`).

## Install / Run

    mkvirtualenv snuba
    ./snuba

## API

Snuba exposes an HTTP API with the following endpoints.

- `/`: Shows this page.
- `/query`: GET endpoint for querying clickhouse.


## Settings

Settings are found in `settings.py`

- `CLICKHOUSE_SERVER` : The HTTP endpoint for the clickhouse service.
- `CLICKHOUSE_TABLE` : The clickhouse table name.
