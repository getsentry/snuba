=======================================
ClickHouse Schema Design Best Practices
=======================================

.. tip::
    This is a work-in-progress document for collecting ClickHouse schema and querying
    best-practices based on experiences running ClickHouse at scale at Sentry.
    It is subject to change and if something doesn't seem right please
    submit a PR to Snuba.

.. contents:: :local:


Columns based on dictionary data (tag promotion)
------------------------------------------------

ClickHouse is a columnar datastore, and at run-time it loads columns on-demand
based on the columns referenced in the query (both the columns ``SELECT`` ed
and those part of the ``WHERE`` clause). The ability to store different columns independently
and not load them for every row for every query is part of the performance advantage that
ClickHouse provides over a traditional RDBMS (like PostgreSQL).

Commonly, a data schema contains a flexible key:value pair mapping
(canonically at Sentry: ``tags`` or ``contexts``) and stores that
data in a ``Nested`` column that contains two arrays where the first array contains the keys
of the dictionary and the second array contains the values. To make queries faster,
a column like this can be indexed with bloom filters as described in :ref:`bloom`. In general
we construct this index across datasets for ``tags`` but not for other columns.

This works well when your dataset and query design gives you the ability to
filter for exact matches and a large number of rows will NOT be an exact match.
Often, however, a ClickHouse query filters for rows that contain a substring match or regular
expression match for a tag value of a given key. This makes bloom filter indexes
not usable for the query and, depending on the other selectivity attributes of your query,
can necessitate moving (or promoting) those relevant values for a given tag key to a new separate
column [#dupe]_.

.. _selectivity:

Selectivity in queries and indices
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Queries are much more efficient when they have the attribute of being low-selectivity on
the table indexes -- meaning the query conditions on indexed columns filter the dataset
to a very small proportion of the overall number of rows. High selectivity
can break the efficiency of the bloom-filter style index on dictionary columns
(see :ref:`bloom`). In cases of high-selectivity queries, there is a negative performance impact on both
bloom-filter indexed columns as well as promoted tag value columns (when searching for a ``key=value``
pair exact match). The promoted column can make the penalty a bit less severe because
it does not load tag values from unrelated keys. Still, an effort should be made to avoid
low-selectivity queries.

.. _bloom:

Bloom filter indexing on dictionary-like columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To facilitate faster searching on dictionary columns, we tend to create bloom filter indices
on a hashes of both the unique ``key`` values of each row as well as hashes of all the ``key=value``
pairs of each row. The `bloom filter <https://en.wikipedia.org/wiki/Bloom_filter>`_  registers these
in a stochastic data structure designed to quickly determine which elements do NOT exist in a set.
So that it can model the entire unbounded keyspace in a fixed amount of memory, a bloom filter
is designed to have false positives. This means that there is actually a performance **penalty**
if the value is often present in the underlying set: First, the value must be tested
against the bloom filter (which will always return "maybe present"), and after
that operation occurs a full scan of the column must be performed.

Due to their structure, bloom filters are only good for exact value searching. They
cannot be used for "is user-value a prefix of column?" or "does column match regex?" style queries.
Those styles of queries require a separate column to search.

.. [#dupe] During migration from non-promoted to promoted, putting the data in both map and
           top-level column may be necessary so that queries of old rows can still access the
           attributes. After the table goes through a full TTL period and the API/storage definition
           is changed to serve the values from the top-level field, message processors should be changed
           to stop writing the data in duplicate places.


Aggregate Tables and Materialization
------------------------------------

A common use case for ClickHouse and Snuba is to ingest raw data and automatically
roll it up to aggregate values (keyed by a custom set of dimensions). This lets
a dataset owner simplify their write logic while getting the query benefits of
rows that are pre-aggregated. This is done with what we'll call a raw table
(the table the consumer writes to), an aggregate table (the table the API reads from)
and a materialized view (which describes how the data should be transformed from
raw to aggregate).

`Sample usage of a materialized view/aggregate table from the official ClickHouse Documentation <https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/aggregatingmergetree#example-of-an-aggregated-materialized-view>`_.
Note that contrary to this example, the aggregate table definition in Snuba is
always separate from the materialized view definition (which is just a ClickHouse SQL
transformation, similar to a PostgreSQL trigger).

In general, Snuba follows the naming conventions here:

* (``widgets_raw_local``, ``widgets_raw_dist``) for the raw (local, distributed) tables
* ``widgets_aggregation_mv`` for the materialized view (this only exists on storage nodes)
* (``widgets_aggregated_local``, ``widgets_aggregated_dist``) for the roll-up/aggregated (local, distributed) tables

Materialized views are immutable so it's normal to have multiple versions of
``widgets_aggregation_mv`` when behavior is updated, with suffixes like
``widgets_aggregation_mv_v1``, ``widgets_aggregation_mv_v2``, etc. Migration
between materialized view versions are described in the next section but in general
old materialized views should be discarded once they are no longer used.

Schema migrations using materialization_version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As we discussed at the end of the prior section, materialized view logic cannot
be updated in place. In order to continuously roll-up input data without data
loss or duplication in the aggregate table, we control logic changes with a
column on the raw table, ``materialization_version``, and making the materialized
view logic depend on specific values of that column. To update MV logic, you
create a new materialized view that looks for the last used value of
``materialization_version`` plus one and then, after that's been created in all
relevant environments, update the consumer to write the new materialization_version
to the raw column.

Here is how this might look in practice:

Context:

1. There is a raw table ``click_events_raw_local``, that has a field named
   ``click_duration``, of type Float64. A snuba consumer is setting this to 0 for
   certain types of click events.
2. There is a materialized view ``click_events_aggregation_mv`` that is writing
   a ``quantilesState()`` value for a ``click_duration`` column in ``click_events_aggregated_local``
   including those zero-values. This materialized view looks for the value of
   ``materialization_version = 0`` in its WHERE condition.
3. The query users are being surprised by p90, p95, and p99 values that are taking into
   account zero-duration click events which don't make sense for the use case.

To resolve this confusion, we don't want to set quantilesState for ``click_duration`` if
the incoming ``click_duration`` is 0.

Steps to resolve the issue:

1. Create a new materialized view ``click_events_aggregation_mv_v1`` via the migration system. This new materialized
   view will use the WHERE clause or some kind of filtering to avoid setting quantilesState(0)
   in the write for the ``click_duration`` column. This new materialized will only operate on
   inputs in ``click_events_raw_local`` where ``materialization_version = 1``
2. Test that this fixes the issue in your local environment by changing your consumer to use
   ``materialization_version = 1``. It can make sense to control this via the settings file in
   (in ``snuba/settings/__init.py__``)
3. Run the migration in all relevant environments.
4. Change the materialization_version setting mentioned above in a specific environment, to
   set ``materialization_version = 1`` on write.
5. Validate that the consumer is writing rows with the new materialization version, and that
   it produces the expected roll-up results.
6. Write a migration to remove the now-unused materialized view (``click_events_aggregation_mv``).
