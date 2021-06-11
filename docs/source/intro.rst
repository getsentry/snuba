Snuba is a service that provides a rich data model on top of Clickhouse
together with a fast ingestion consumer and a query optimizer.

Snuba was originally developed to replace a combination of Postgres and
Redis to search and provide aggregated data on Sentry errors.
Since then it has evolved into the current form where it supports most
time series related Sentry features over several data sets.

Features:
---------

* Provides a database access layer to the Clickhouse distributed data store.
* Provides a graph logical data model the client can query through the SnQL
  language which provides functionalities similar to those of SQL.
* Support multiple separate data sets in a single installation.
* Provides a rule based query optimizer.
* Provides a migration system to apply DDL changes to Clickhouse both in a
  single node and distributed environment.
* Ingest data directly from Kafka
* Supports both point in time queries and streaming queries.


Some use cases in Sentry:
-------------------------

* The ``events`` data set powers features like the Issue Page. Here the search
  functionality is powered by Snuba as well as all the aggregations.
* The ``discover`` data set powers all the Performance Monitoring related
  features.
* The ``sessions`` data set powers the Releases feature. Specifically this
  data set ingests a much higher volume of data points and stores pre-aggregated
  data to allow fast queries over higher volume of data.
* The ``outcomes`` data set powers the Stats page.
