======================
Snuba migration modes
======================

Snuba runs migrations against a single node ClickHouse (local mode) or a
replicated, sharded cluster (distributed mode). The mode comes from
``settings.CLUSTERS``. See :doc:`overview` for the settings.

Local mode
==========

Local mode is the default. The ClickHouse container that ``devservices up``
starts is a single node, and the default settings use it::

    devservices up
    snuba migrations migrate --force

Distributed mode
================

Distributed mode needs a ClickHouse cluster with ZooKeeper. The devservices
ClickHouse is a single node, so it cannot run distributed migrations.

To run the distributed migration tests locally, run::

    make test-distributed-migrations

The target builds a Snuba image and starts the ``multi_node`` profile of
``docker-compose.gcb.yml``. That profile runs ZooKeeper, a query node, and four
storage nodes, and runs the tests in ``test_distributed_migrations/``.

To run migrations against your own cluster, set ``SNUBA_SETTINGS=distributed``.
This loads ``snuba/settings/settings_distributed.py``, which assigns every
storage set to the one-shard cluster ``cluster_one_sh``::

    SNUBA_SETTINGS=distributed snuba migrations migrate --force

More information about distributed tables is in the
`ClickHouse documentation <https://clickhouse.com/docs/engines/table-engines/special/distributed>`_.
