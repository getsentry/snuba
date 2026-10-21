==========================
Getting started with Snuba
==========================

This guide starts Snuba in a Sentry development environment.

Requirements
------------

Snuba needs:

1. A ClickHouse server at ``CLICKHOUSE_HOST`` (default ``127.0.0.1``).
2. A Redis instance at ``REDIS_HOST`` (default ``127.0.0.1``) on ``REDIS_PORT``
   (default ``6379``).
3. A Kafka cluster at ``DEFAULT_BROKERS`` (default ``127.0.0.1:9092``).

Snuba assumes that everything runs on UTC. Other timezones cause timezone
mismatches.

Start Snuba with Sentry
-----------------------

With a Sentry development environment set up, start the services from the
Sentry repository::

    devservices up

This runs Snuba in a container. To run Snuba from your Snuba checkout instead,
switch it to local mode::

    devservices toggle snuba local

To switch back to the container, run::

    devservices toggle snuba containerized

Start only Snuba's dependencies
-------------------------------

From the Snuba repository, start ClickHouse, Redis, and Kafka::

    devservices up

Then start Snuba itself as described in :doc:`/contributing/environment`.

Query ClickHouse directly
-------------------------

To open a ClickHouse client (similar to ``psql``), run::

    docker exec -it snuba-clickhouse-1 clickhouse-client

Errors are written to the ``errors_local`` table::

    select count() from errors_local;

Settings
--------

Settings are in ``snuba/settings/__init__.py``. To override them, set
``SNUBA_SETTINGS`` to a short name such as ``test``, which loads
``snuba/settings/settings_test.py``, or to an absolute path to a settings file.

- ``CLUSTERS``: The ClickHouse clusters, with the host, port, and storage sets
  for each. Each cluster also sets whether it is single node or distributed.
- ``REDIS_HOST``: The host Redis runs on.
