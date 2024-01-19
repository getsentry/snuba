=======================
The MQL query language
=======================

This document describes the Metrics Query Language (MQL). For more details on
how to actually send a query to Snuba see :doc:`/query/overview`. The full grammar
can be found in the ``snuba-sdk`` repository: `https://github.com/getsentry/snuba-sdk/blob/main/snuba_sdk/mql/mql.py`_.

Queries are composed at their core of timeseries, which are aggregate functions on a metric::

    aggregate ( metric_name / mri )

    e.g.

    sum(transaction.duration)
    quantiles(0.99)(d:sessions/duration@second)

Timeseries can also be filtered and grouped by tags::

    aggregate ( metric_name ) { tagkey:tagvalue } by tagkey

    e.g.

    sum(transaction.duration){environment:prod} by status_code

These timeseries can then be combined with arithmetic operations or referenced in custom functions::

    timeseries arithmetic_op timeseries

    function_name ( timeseries / parameter (, timeseries / parameter)* )

    e.g.

    sum(transaction.duration) + max(transaction.measurements.fcp)
    apdex(sum(transaction.duration), 300)

Formulas, functions and timeseries all support filters and grouping and can be combined in almost any way::

    count(transaction.duration){!status_code:200} / count(transaction.duration) # failure rate

    apdex(sum(transaction.duration) + sum(transaction.measurements.fcp), 300){environment:prod} by status_code

However, there is information about the query that is not contained in the query string above. That's why a MQL query
to Snuba also contains a ``mql_context`` dictionary::

    {
        "mql": "sum(transaction.duration)",
        "mql_context": {
            "entity": "generic_metrics_distributions",
            "start": "2023-01-02T03:04:05+00:00",
            "end": "2023-01-16T03:04:05+00:00",
            "rollup": {
                "orderby": None,
                "granularity": 3600,
                "interval": 3600,
                "with_totals": None,
            },
            "scope": {
                "org_ids": [1],
                "project_ids": [11],
                "use_case_id": "transactions",
            },
            "limit": 100,
            "offset": 5,
            "indexer_mappings": {},
        },
    }

The Snuba SDK provides convenience classes around the more complex fields.


start / end
===========
The start and end fields are ISO 8601 timestamps. They are required for all queries. In the SDK, a ``MetricsQuery`` object has a ``start`` and ``end``
field.

limit / offset
==============
Specified in the ``MetricsQuery`` object with the ``limit`` and ``offset`` fields. These are optional and default to 1000 and 0 respectively.

Rollup
======

See `here https://github.com/getsentry/snuba-sdk/blob/main/snuba_sdk/timeseries.py`_ for the full object.

The rollup object contains information about how the timeseries should be grouped. The ``interval`` field specifies what time interval the timeseries should be grouped by.
The ``total`` field serves two purposes: if the ``interval`` is set, ``total`` determines whether a totals row should be provided alongside the timeseries.
Otherwise, it means that instead of a timeseries, a single value should be returned (or one per tag value, if the query is grouped by a tag). In this case, ``orderby`` can be used
to specify how the results should be ordered. ``granularity`` is inferred automatically from the start/end times and the interval, and aligns with the time buckets stored in Clickhouse.

Scope
=====

The scope object contains information about the query that is not represented in tags. Currently that is project ID(s), organization ID(s) and use case ID.
The use case ID is automatically inferred based on the MRI of the metrics in the query. Queries across use case IDs are not supported.
