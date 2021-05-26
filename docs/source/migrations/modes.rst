======================
Snuba Migration Modes
======================

This document outlines a way to try out distributed migrations.
Note that this is experimental, and should be used only for development
purposes at the moment. Distributed mode is not supported when testing yet.
Local mode for migrations is currently fully supported.

If you are running ClickHouse via Sentry's devservices, the
main "switch" between the two modes for running data migrations (local and
distributed) lives in ``sentry/conf/server.py``.
The controlling envrionment variable is ``SENTRY_DISTRIBUTED_CLICKHOUSE_TABLES``,
and its value must be set in order to use a specific mode.

Once this boolean variable is set, one of two ClickHouse Docker volumes will be
used for data storage, depending on the mode (distributed or local). Whenever a user
wants to switch between the two modes, they must "turn off" the running ClickHouse
container, alter the environment variable mentioned above, and then "turn on" the
same container to be in the new mode.

More information on migrations in general can be found `here <https://github.com/getsentry/snuba/blob/master/MIGRATIONS.md>`_.

Enabling Local Mode
=====================

In your local ``server.py``, set ``SENTRY_DISTRIBUTED_CLICKHOUSE_TABLES``
to False. This is the default setting, so configuration is already
set up for local mode migrations. Start up the corresponding ClickHouse
container (``sentry devservices up clickhouse``).

Now, run migrations as expected (``snuba migrations migrate --force``).


Enabling Distributed Mode
============================

In your local ``server.py``, set ``SENTRY_DISTRIBUTED_CLICKHOUSE_TABLES``
to True. Start up the corresponding ClickHouse container (``sentry devservices up clickhouse``).
Make sure that the Zookeeper container is also running; without it, distributed migrations
will not work properly.

Now, we take a look at the cluster configurations that can be used in Distributed tables. These are
set in `this config <https://github.com/getsentry/sentry/blob/master/config/clickhouse/dist_config.xml>`_.
The current configuration in the file is a default, 1 shard with 1 replica model, and is best to use
for now, as it supports migrations for all storages. Moving forward, we look to adding support
for multi-sharded configurations, and ensuring storages are placed on the right clusters.
More examples of and information on cluster configurations can be found in `this link <https://clickhouse.tech/docs/en/engines/table-engines/special/distributed/>`_.

Finally, set up cluster connection details (for example, which storage is to be assigned
to be which cluster) in `this file <https://github.com/getsentry/snuba/blob/master/snuba/settings/settings_distributed.py>`_.
This needs to be done only for distributed migrations, as the default cluster details will be used in local mode.
The default in this file works with the default cluster configurations mentioned above, so no changes
are immediately necessary.

Now, run migrations with the ``SNUBA_SETTINGS`` environment variable pointing to distributed mode.
This can be done as follows: ``SNUBA_SETTINGS=distributed snuba migrations migrate --force``.
