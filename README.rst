.. image:: https://github.com/getsentry/snuba/raw/master/snuba/admin/dist/snuba.svg
    :width: 280

.. intro-start

Snuba is a service that provides a rich data model on top of ClickHouse
together with a fast ingestion consumer and a query optimizer.

Snuba was originally developed to replace a combination of Postgres and
Redis to search and provide aggregated data on Sentry errors.
Since then it has evolved into the current form where it supports most
time series related Sentry features over several data sets.

Features:
---------

* Provides a database access layer to the ClickHouse distributed data store.
* Provides a graph logical data model the client can query through the SnQL
  language which provides functionalities similar to those of SQL.
* Supports multiple separate data sets in a single installation.
* Provides a rule based query optimizer.
* Provides a migration system to apply DDL changes to ClickHouse both in a
  single node and distributed environment.
* Ingests data directly from Kafka.
* Supports both point in time queries and streaming queries.

.. intro-end

Get started
-----------

To run Snuba in a Sentry development environment, see
`Getting started <https://getsentry.github.io/snuba/getstarted.html>`_.
To query it, see `Querying Snuba <https://getsentry.github.io/snuba/query/overview.html>`_.

The full documentation is at https://getsentry.github.io/snuba/.

Get help
--------

Report bugs and request features in
`GitHub issues <https://github.com/getsentry/snuba/issues>`_.

License
-------

Snuba is licensed under the Functional Source License, Version 1.1, Apache 2.0
Future License. See `LICENSE.md <LICENSE.md>`_.
