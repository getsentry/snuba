Clickhouse Queries Of Death
===========================


The following queries have been shown to segfault ClickHouse on 20.7 (which is the minimum Clickhouse version of Snuba). Do not run these queries in the tracing tool, unless you really want to take ClickHouse down.

countif(”DOOM”)
---------------

Query ::

    SELECT countIf(environment='production')
    FROM ...
    PREWHERE environment = 'production'

A ``countif`` in the ``SELECT`` with that same condition in the ``PREWHERE`` will segfault ClickHouse. This will be fixed in 21.8 when the upgrade is complete
