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
data in a column that contains two ``Nested`` arrays where the first array contains the keys
of the dictionary and the second array contains the values, indexed by a bloom filter. This works well when
your dataset and query design gives you the ability to filter for exact matches to a small
number of rows and the flexibility of completely arbitrary keys and values are a real requirement.
Often, however, a ClickHouse query filters for rows that contain a substring match or regular
expression match for a tag value of a given key. This makes bloom filter (see :ref:`bloom`) indexes
not usable for the query and requires moving (or promoting) those relevant values for a given tag to
a new separate column which can support these queries more efficiently [#dupe]_.

It is very efficient for queries to filter by a specific matching value that exists in
a low number of rows for a given tag key being non-empty or key:value pair on a column
that contains **just** the values of that specific key. However, in :ref:`selectivity`
we discuss why this fails when queries contain conditions for a specific ``key=value``
pair that occurs in a very large percentage of the table data (for example,
``http_status_code=200`` or ``hasKey(http_status_code)``).

.. _selectivity:

Selectivity in queries and indices
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Queries are much more efficient when they have the attribute of being low-selectivity on
the table indexes -- meaning the query conditions on indexed columns filter the dataset
to a very small proportion of the overall number of rows. Therefore it's ideal to design
schemas and queries that cause searches on a very low number of rows. High selectivity
breaks any efficiency of the bloom-filter style index on dictionary columns (see :ref:`bloom`).

In cases of high-selectivity queries, there is a negative performance impact on both
bloom-filter indexed columns as well as promoted tag value columns (when searching for a ``key=value``
pair exact match). The promoted column can make the penalty a bit less severe because
it does not load tag values from unrelated keys. Still, an effort should be made to avoid
any low-selectivity queries.

.. _bloom:

Bloom filter indexing on dictionary-like columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To facilitate faster searching on dictionary columns, we tend to create bloom filter indices
on a hash of both the unique ``key`` values of each row as well as the ``key=value`` pairs
of each row. The `bloom filter <https://en.wikipedia.org/wiki/Bloom_filter>`_ index that stores these
is a stochastic data structure designed to quickly determine which elements do NOT exist in a set,
but there is actually a performance **penalty** if the value is often present in the underlying set.
First, the value must be tested against the bloom filter (which will always return "maybe
present"), and after that operation occurs a full scan of the column must be performed.

When searching a dictionary column for a value likely to filter many rows (for example,
``user:browser=internet_explorer6``) this is an effective filter. Searching for ``user:os=android``,
however, could mean that your query will approximate a full column scan. Similarly, if querying
for ``hasTag(user:browser)`` and 80% of your users have browser tags, that will result in a full
column scan filled with unrelated data.

Additionally, due to their structure, bloom filters are only good for exact value searching. They
cannot be used for "is user-value a prefix of column?" or "does column match regex?" style queries.
Those styles of queries require a separate column to search.

.. [#dupe] During migration from non-promoted to promoted, putting the data in both map and
           top-level column may be necessary so that queries of old rows can still access the
           attributes. After the table goes through a full TTL period and the API/storage definition
           is changed to serve the values from the top-level field, message processors should be changed
           to stop writing the data in duplicate places.
