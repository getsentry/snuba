ClickHouse Schema Design Best Practices
=======================================

Columns based on dictionary data
--------------------------------

ClickHouse is a columnar datastore, and it loads columns on-demand based on the columns referenced
in the query (both the columns ``SELECT``ed and those part of the ``WHERE`` clause). Commonly,
a data schema contains a flexible key:value pair mapping (for example: tags) and stores that
data in a column that contains two ``Nested`` arrays where the first array contains the keys
of the dictionary and the second array contains the values. This works well when
your dataset design gives you the ability to filter based on other attributes to a small
number of rows and the flexibility of completely arbitrary keys and values are a real requirement.
Often, however, a ClickHouse user is interested in rows that contain a specific tag key or a
specific ``key=value`` pair and in this case one is often best-served by creating a top-level
column and duplicating (we often call it "promoting" in code) the data.

This is more efficient on two dimensions:

1. A dictionary style-column can be arbitrarily sized and, especially if you know you will often only
need a single value from the dictionary, it may be that just returning
this column in your results blows up the size of your **query results** result in-memory. It also can
exhaust the cache and in that manner slow down other unrelated queries.

2. Queries that filter based on an array column have to load the entire column into memory
in order to do filtering. This makes it more difficult and expensive for the query planner to build
indexing on the presence of a key.
