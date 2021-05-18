======================
Snuba Migration Modes
======================

The main "switch" between the two modes for running data migrations (local and
distributed) lives in a user's local Sentry configuration (in their ``sentry.conf.py``).
The controlling envrionment variable is ``SENTRY_DISTRIBUTED_CLICKHOUSE_TABLES``,
and its default value is set `at this line <https://github.com/getsentry/sentry/blob/master/src/sentry/conf/server.py#L127>`_.

Once this boolean variable is set, one of two Clickhouse Docker containers will be
(allowed to) start, depending on the mode (distributed or local).

More information on migrations in general can be found `here <https://github.com/getsentry/snuba/blob/master/MIGRATIONS.md>`_.

Enabling Local Mode
=======================

In your local Sentry environment configuration, set ``SENTRY_DISTRIBUTED_CLICKHOUSE_TABLES``
to False. Start up the corresponding Clickhouse container (``sentry devservices up clickhouse``).

Ensuring that your local configuration is prepared for migrations (reverting any existing
migrations, or dropping certain databases/tables), run migrations as expected
(``snuba migrations migrate --force``).


Enabling Distributed Mode
==========================

In your local Sentry environment configuration, set ``SENTRY_DISTRIBUTED_CLICKHOUSE_TABLES``
to True. Start up the corresponding Clickhouse container (``sentry devservices up clickhouse_dist``).

Now, it is important to configure the clusters that can be used in Distributed tables. These are
set in `this file <https://github.com/getsentry/sentry/blob/master/config/clickhouse/dist_config.xml>`_.
Moreover, examples of and information on cluster configurations can be found `here <https://clickhouse.tech/docs/en/engines/table-engines/special/distributed/>`_.

Finally, set up cluster connection details (for example, which storage is to be assigned
to be which cluster) in `this file <https://github.com/getsentry/snuba/blob/master/snuba/settings/settings_distributed.py>`_.
This needs to be done only for distributed migrations, as the default cluster details will be used in local mode.

Ensuring that your local configuration is prepared for migrations (reverting any existing
migrations, or dropping certain databases/tables), run migrations with the ``SNUBA_SETTINGS``
environment variable pointing to distributed mode. This can be done as follows:
``SNUBA_SETTINGS=distributed snuba migrations migrate --force``.
