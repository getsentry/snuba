===========================
Snuba Architecture Overview
===========================

Snuba is a time series oriented data store backed by
`Clickhouse <https://clickhouse.tech/>`_, which is a columnary storage
distributed database well suited for the kind of queries Snuba serves.

Data is fully stored in Clickhouse tables and materialized views,
it is ingested through input streams (only Kafka topics today)
and can be queried either through point in time queries or through
streaming queries (subscriptions).

.. image:: /_static/architecture/overview.png

Storage
=======

Clickhouse was chosen as backing storage because it provides a good balance
of the real time performance Snuba needs, its distributed and replicated
nature, its flexibility in terms of storage engines and consistency guarantees.

Snuba data is stored in Clickhouse tables and Clickhouse materialized views.
Multiple Clickhouse `storage engines <https://clickhouse.tech/docs/en/engines/table-engines/>`_
are used depending on the goal of the table.

Snuba data is organized in multiple Datasets which represent independent
partitions of the data model. More details in the :doc:`/architecture/datamodel`
section.

Ingestion
=========

Snuba does not provide an api endpoint to insert rows (except when running
in debug mode). Data is loaded from multiple input streams, processed by
a series of consumers and written to Clickhouse tables.

A consumer consumes one one or multiple topics and writes on one or multiple
tables. No table is written onto by multiple consumers as of today. This
allows some consistency guarantees discussed below.

Data ingestion is most effective in batches (both for Kafka but especially
for Clickhouse). Our consumers support batching and guarantee that one batch
of events taken from Kafka is passed to Clickhouse at least once. By properly
selecting the Clickhouse table engine to deduplicate rows we can achieve
exactly once semantics if we accept eventual consistency.

Query
=====

The simplest query system is point in time. Queries are expressed in a
the SnQL language (:doc:`/language/snql`) and are sent as post HTTP calls.
The query engine processes the query (process described in
:doc:`/architecture/queryprocessing`) and transforms it into a ClickHouse
query.

Streaming queries (done through the Subscription Engine) allow the client
to receive query results in a push way. In this case an HTTP endpoint allows
the client to register a streaming query. Then The Subscription Consumer consumes
to the topic that is used to fill the relevant Clickhouse table for updates,
periodically runs the query through the Query Engine and produces the result
on the subscriptions Kafka topic.

Data Consistency
================

Different consistency models coexist in Snuba to provide different guarantees.

By default Snuba is eventually consistent. When running a query, by default,
there is no guarantee of monotonic reads since Clickhouse is multi-leader
and a query can hit any replica and there is no guarantee the replicas will
be up to date. Also, by default, there is no guarantee Clickhouse will have
reached a consistent state on its own.

It is possible to achieve strong consistency on specific query by forcing
Clickhouse to reach consistency before the query is executed (FINAL keyword),
and by forcing queries to hit the specific replica the consumer writes onto.
This essentially uses Clickhouse as if it was a single leader system and it
allows Sequential consistency.
