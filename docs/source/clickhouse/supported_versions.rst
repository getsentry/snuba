=============================
ClickHouse supported versions
=============================
The following version(s) of Clickhouse have been tested and are known to work
with Snuba:

- 25.8.16.10001 (Altinity Stable Build)
- 25.8.33.10002 (Altinity Stable Build)

Any version of Clikhouse used outside of this list could potentially work,
but is not guaranteed to work. Some functionality might be broken. Use a
different version at your own risk. There are plans to support more recent
versions of Clickhouse in the future. When Snuba has been validated to work
with the new versions of Clickhouse, this list will be updated.

Mixed-version clusters
======================
A rolling upgrade leaves the cluster mixed for a while: some nodes on the old
version and some on the new. Distributed queries then cross versions, and that
can fail even when each version works on its own. For example, 26.5 and later
quote dotted Nested column names in the block headers that query and storage
nodes exchange, while earlier versions don't (`ClickHouse#113250
<https://github.com/ClickHouse/ClickHouse/issues/113250>`_).

The ``clickhouse-mixed-versions`` CI job covers this. It starts the multi-node
cluster from ``docker-compose.gcb.yml`` with the query node and the storage
shards on different images, then runs the queries in
``test_distributed_migrations/test_mixed_version_queries.py``. To try a pair
locally, set ``CLICKHOUSE_QUERY_IMAGE`` and ``CLICKHOUSE_STORAGE_IMAGE``, and
optionally ``CLICKHOUSE_STORAGE_02_IMAGE`` or ``CLICKHOUSE_STORAGE_03_IMAGE`` to
put one shard on a different version. Unset ones fall back to
``CLICKHOUSE_IMAGE``.
Set ``CLICKHOUSE_REMOTE_SERVERS_CONFIG`` to
``./test_distributed_migrations/config/clickhouse/remote_servers_single_shard.xml``
to run with a single storage shard instead of two.
