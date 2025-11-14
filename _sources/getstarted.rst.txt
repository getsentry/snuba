==========================
Getting started with Snuba
==========================

This is a guide to quickly start Snuba up in the context of a Sentry
development environment.

Requirements
------------

Snuba assumes:

1. A Clickhouse server endpoint at ``CLICKHOUSE_HOST`` (default ``127.0.0.1``).
2. A redis instance running at ``REDIS_HOST`` (default ``127.0.0.1``). On port
   `6379`
3. A Kafka cluster running at ``127.0.0.1`` on port `9092`.

A quick way to get these services running is to set up sentry, and add the following line
in ``~/.sentry/sentry.conf.py``::

    SENTRY_EVENTSTREAM = "sentry.eventstream.kafka.KafkaEventStream"

And then use::

    devservices up --exclude=snuba

Note that Snuba assumes that everything is running on UTC time. Otherwise
you may experience issues with timezone mismatches.


Sentry + Snuba
--------------

Add/change the following lines in ``~/.sentry/sentry.conf.py``::

    SENTRY_SEARCH = 'sentry.search.snuba.EventsDatasetSnubaSearchBackend'
    SENTRY_TSDB = 'sentry.tsdb.redissnuba.RedisSnubaTSDB'
    SENTRY_EVENTSTREAM = 'sentry.eventstream.snuba.SnubaEventStream'

Run::

    sentry devservices up

Access raw clickhouse client (similar to psql)::

    docker exec -it sentry_clickhouse clickhouse-client

Data is written into the table `sentry_local`: `select count() from sentry_local;`

Settings
--------

Settings are found in ``settings.py``

- ``CLUSTERS`` : Provides the list of clusters and the hostname, port, and storage sets that should run on each cluster. Local vs distributed is also set per cluster.
- ``REDIS_HOST`` : The host redis is running on.
