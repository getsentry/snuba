ClickHouse Schema Design Best Practices
=======================================

Columns based on dictionary data
--------------------------------

ClickHouse is a columnar datastore, and at run-time it loads columns on-demand
based on the columns referenced in the query (both the columns ``SELECT`` ed
and those part of the ``WHERE`` clause). The ability to store different columns independently
and not load them for every row for every query is part of the performance advantage that
ClickHouse provides over a traditional RDBMBS (like PostgreSQL).

Commonly, a data schema contains a flexible key:value pair mapping (canonically: ``tags``) and stores that
data in a column that contains two ``Nested`` arrays where the first array contains the keys
of the dictionary and the second array contains the values. This works well when
your dataset design gives you the ability to filter based on other attributes to a small
number of rows and the flexibility of completely arbitrary keys and values are a real requirement.
Often, however, a ClickHouse user is interested in rows that contain a specific tag key or a
specific ``key=value`` pair. In this case one is often best-served by creating a top-level
column and "promoting" the data to it [#dupe]_.

This promotion is preferable on a couple dimensions:

1. A dictionary style-column can be arbitrarily sized and, especially if you know you will often only
   need a single value from the dictionary, it may be that just returning
   this column in your results blows up the amount of data that has to be processed, aggregated
   and serialized in-memory. It also can exhaust the cache and in that manner slow down other unrelated queries.
2. Queries that filter based on an array column may have to load the entire column into memory
   in order to do filtering. Adding a new index on top of a top-level column is a more
   straightforward operation that guarantees more consistent performance outcomes vs. iterating
   over an array

.. [#dupe] During migration from non-promoted to promoted, putting the data in both map and
           top-level column may be necessary so that queries of old rows can still access the
           attributes. After the table goes through a full TTL period, however, users should
           stop writing the data in duplicate places.

..
   # (TODO: add some information to the above section about how we have
   done indexes on arrays, and when that might be appropriate)
