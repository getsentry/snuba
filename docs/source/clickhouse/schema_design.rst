=======================================
ClickHouse Schema Design Best Practices
=======================================


Columns based on dictionary data (tag promotion)
------------------------------------------------

ClickHouse is a columnar datastore, and at run-time it loads columns on-demand
based on the columns referenced in the query (both the columns ``SELECT`` ed
and those part of the ``WHERE`` clause). The ability to store different columns independently
and not load them for every row for every query is part of the performance advantage that
ClickHouse provides over a traditional RDBMS (like PostgreSQL).

Commonly, a data schema contains a flexible key:value pair mapping
(canonically: ``tags`` or ``contexts``) and stores that
data in a column that contains two ``Nested`` arrays where the first array contains the keys
of the dictionary and the second array contains the values. This works well when
your dataset design gives you the ability to filter based on other attributes to a small
number of rows and the flexibility of completely arbitrary keys and values are a real requirement.
Often, however, a ClickHouse user is predominantly interested in rows that contain a specific tag key or a
specific ``key=value`` pair that occurs in a very large percentage of the table data (for example,
``http_status_code=200`` or ``hasKey(http_status_code)``). This breaks any efficiency
of the bloom-filter style index on dictionary columns (see :ref:`bloom`).
In this case one is often best-served by creating a top-level column and "promoting"
the data to it [#dupe]_.

It is very efficient for queries to filter by a specific matching value that exists in
a high number of rows for a given tag key being non-empty or key:value pair on a new
indexed column. If we know the tag key is low-selectivity, we can get a significant performance
improvement with a relatively straightforward schema modification. Note that this does
require creating an index on the new promoted column.

.. _bloom:

Bloom filter indexing on dictionary-like columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To facilitate faster searching on dictionary columns, we tend to create bloom filter indices
on a hash of both the unique ``key`` values of each row as well as the ``key=value`` pairs
of each row. The `bloom filter <https://en.wikipedia.org/wiki/Bloom_filter>`_ index that stores these
is a stochastic data structure designed to quickly determine which elements do NOT exist in a set,
but provides little to no optimization if a value is often present in the underlying set.

When searching a dictionary column for a value likely to filter many rows (for example,
``user:browser=internet_explorer6``) this is an effective filter. Searching for ``user:os=android``,
however, could mean that your query will approximate a full column scan. Similarly, if querying
for ``hasTag(user:browser)`` and 80% of your users have browser tags, that will result in a full
column scan filled with unrelated data.

.. [#dupe] During migration from non-promoted to promoted, putting the data in both map and
           top-level column may be necessary so that queries of old rows can still access the
           attributes. After the table goes through a full TTL period and the API/storage definition
           is changed to serve the values from the top-level field, message processors should be changed
           to stop writing the data in duplicate places.
