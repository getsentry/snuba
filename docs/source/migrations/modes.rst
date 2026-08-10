======================
Snuba Migration Modes
======================

This document outlines a way to try out distributed migrations.
Note that this is experimental, and should be used only for development
purposes at the moment. Distributed mode is not supported when testing yet.
Local mode for migrations is currently fully supported.

If you are running ClickHouse via Sentry's devservices, the main "switch"
between the two modes for running data migrations (local and distributed) is
``SENTRY_DISTRIBUTED_CLICKHOUSE_TABLES``. Its default is defined in
`src/sentry/conf/server.py <https://github.com/getsentry/sentry/blob/master/src/sentry/conf/server.py>`_,
and you can override it in ``~/.sentry/sentry.conf.py``.

Once this boolean variable is set, one of two ClickHouse Docker volumes will be
used for data storage, depending on the mode (distributed or local). Whenever a user
wants to switch between the two modes, they must "turn off" the running ClickHouse
container, alter the environment variable mentioned above, and then "turn on" the
same container to be in the new mode.

More information on migrations in general can be found `here <https://github.com/getsentry/snuba/blob/master/MIGRATIONS.md>`_.

Enabling Local Mode
=====================

In ``~/.sentry/sentry.conf.py``, set ``SENTRY_DISTRIBUTED_CLICKHOUSE_TABLES``
to False. This is the default setting, so configuration is already
set up for local mode migrations. Start up the corresponding ClickHouse
container (``devservices up clickhouse``).

Now, run migrations as expected (``snuba migrations migrate --force``).


Enabling Distributed Mode
============================

In ``~/.sentry/sentry.conf.py``, set ``SENTRY_DISTRIBUTED_CLICKHOUSE_TABLES``
to True. Start up the corresponding ClickHouse container (``devservices up clickhouse``).
Make sure that the Zookeeper container is also running; without it, distributed migrations
will not work properly.

Set up cluster connection details (for example, which storage is assigned to
which cluster) in
`snuba/settings/settings_distributed.py <https://github.com/getsentry/snuba/blob/master/snuba/settings/settings_distributed.py>`_.
This is needed only for distributed migrations. The default configuration uses
a one-shard cluster and supports migrations for all storages. More information
about distributed tables is available in the
`ClickHouse documentation <https://clickhouse.com/docs/engines/table-engines/special/distributed>`_.

Now, run migrations with the ``SNUBA_SETTINGS`` environment variable pointing to distributed mode.
This can be done as follows: ``SNUBA_SETTINGS=distributed snuba migrations migrate --force``.
