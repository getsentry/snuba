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
- [/dashboard](/dashboard): Query dashboard
- [/query](/query): Endpoint for querying clickhouse.
- [/config](/config): Console for runtime config options

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

## Querying

### Tags

Event tags are stored in one of 2 ways. Promoted tags are the ones we expect to
be queried often and as such are stored as top level columns. The list of
promoted tag columns is defined in settings and is somewhat fixed in the
schema. The rest of an event's tags are stored as a key-value map.  In practice
this is implemented as 2 columns of type Array(String), called `tags.key` and
`tags.value`

The snuba service provides 2 mechanisms for abstracting this tiered tag
structure by providing some special columns that will be resolved to the
correct SQL expression for the type of tag. These mechanisms should generally
not be used in conjunction with each other.

#### When you know the names of the tags you want.

You can use the `tags[name]` anywhere you would use a normal column name in an
expression, and it will resolve to the value of the tag with `name`, regardless
of whether that tag is promoted or not. Use this syntax when you are looking
for a specific named tag. eg.

    # Find all events in production with user_custom_key defined.
    "conditions": [
        ["tags[environment]", "=", "prod"],
        ["tags[custom_user_tag]", "IS NOT NULL"]
    ],
<!-- -->

    # Find the number of unique environments
    "aggregations": [
        ["uniq", "tags[environment]", "unique_envs"],
    ],

#### When you don't know the name, or want to query all tags.

These are virtual columns that can be used to get results when the names of the
tags are not explicitly known. Using `tags_key` or `tags_value` in an
expression will expand all of the promoted and non-promoted tags so that there
is one row per tag (an array-join in Clickhouse terms). For each row, the name
of the tag will be in the `tags_key` column, and the value in the `tags_value`
column.

    # Find the top 5 most often used tags
    "aggregations": [
        ["topK(5)", "tags_key", "top_tag_keys"],
    ],
<!-- -->

    # Find any tags whose *value* is `bar`
    "conditoons": [
        ["tags_value", "=", "bar"],
    ],


Note, when using this expression. the thing you are counting is tags, not events, so if you
have 10 events, each of which has 10 tags, then a `count()` of `tags_key` will return 100.
