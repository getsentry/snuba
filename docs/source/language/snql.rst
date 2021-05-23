=======================
The SnQL query language
=======================

This document describes the Snuba Query Language (SnQL). For more details on
how to actually send a query to Snuba see :doc:`/query/overview`.

This is the query structure.::

    MATCH simple | join | subquery
    SELECT [expressions] | [aggregations BY expressions]
    ARRAY JOIN [column]
    WHERE condition [[AND | OR] condition]*
    HAVING condition [[AND | OR] condition]*
    ORDER BY expressions ASC|DESC [, expressions ASC|DESC]*
    LIMIT expression BY n
    LIMIT n
    OFFSET n
    GRANULARITY n
    TOTALS boolean


These queries are sent as strings to the ``/:dataset/snql`` endpoint encoded in a
JSON body of the form below.::

    {
        "query": "<query>",
        "dataset": "<dataset>",
        "consistent": bool,
        "turbo": bool,
        "debug": bool,
    }

The dataset is implied through the url used for the query. All of the fields except
for ``query`` are optional in the JSON body.

MATCH
=====

Our data model is represented by a graph of entities. This clause identifies
the pattern of the subgraphs we are querying on. There are three types of
MATCH clause that are currently supported:

**Simple:**

``MATCH (<entity> [SAMPLE n])``

This is equivalent to all of our current queries. This is querying data from
a single entity (Errors, Transactions etc.) It is possible to add an optional
sample to the query by adding it with the entity.

Example ``MATCH (errors)``.

**Subquery:**

``MATCH { <query> }``

Inside the curly braces can be another SnQL query in its entirety. Anything
in the SELECT/BY clause of the subquery will be exposed in the outer query
using the aliases specified.

Example::

    MATCH {
        MATCH (transactions)
        SELECT avg(duration) AS avg_d BY transaction
    }
    SELECT max(avg_d)

**Join:**

``MATCH (<alias>: <entity> [SAMPLE n]) -[<join>]-> (<alias>: <entity> [SAMPLE n])``

A join represents a multi node subgraph is a subgraph that includes
multiple relationships between different nodes. We only support 1..n,
n..1 and 1..1 directed relationships between nodes.

With JOINs every entity must have an alias, which is a unique string.
Sampling can also be applied to any of the entities in the join. The
``<join>`` is a string that is specified in the Entity in Snuba, and
is a short hand for a set of join conditions. It's possible to have more
than one join clause, separated by commas.

Example::

    MATCH
        (e: events) -[grouped]-> (g: groupedmessage),
        (e: events) -[assigned]-> (a: groupassignee)
    SELECT count() AS tot BY e.project_id, g.id
    WHERE a.user_id = "somebody"

The type of join (left/inner) and the join key are part of the data model
and not part of the query. They are hard coded in the entity code.
This is because not entity can be safely joined with any other entity
in the distributed version of the underlying database.

The tuples provided by the match clause to the where clause look exactly
like the ones produced by conventional join clause.::

    [
        {"e.project_id": 1,  "g.id": 10}
        {"e.project_id": 1,  "g.id": 11}
        {"e.project_id": 2,  "g.id": 20}
        ...
    ]


SELECT .. BY
============

This clause specifies which results should be returned in the output.
If there is an aggregation, when everything in the ``BY`` clause is
treated as a grouping key.
It is possible to have aggregations without a ``BY`` clause if we want
to aggregate across the entire result set, but, in such case, nothing
other than the aggregation can be in the ``SELECT``.
It's not valid to have
an empty ``SELECT`` clause, even if there is a ``BY`` clause.

Expressions in the SELECT clause can be columns, arithmetic, functions
or any combination of the three. If the query is a join, then each column
must have a qualifying alias that matches one of the entity aliases in the
MATCH clause.

WHERE
=====

This is the filter of the query that happens **before** aggregations (like
the WHERE in SQL).

Conditions are infix expressions of the form ``LHS OP RHS*``, where ``LHS``
and ``RHS`` are literal values or expressions. ``OP`` refers to a specific
operator to compare the two values. These operators are one of
``=, !=, <, <=, >, >=, IN, NOT IN, LIKE, NOT LIKE, IS NULL, IS NOT NULL``.
Note that the ``RHS`` is optional when using an operator like ``IS NULL``.

Conditions can be combined using the boolean keywords ``AND`` or ``OR``.
They can also be grouped using ``()``.

Some conditions will be mandatory to provide a valid query depending on
the entity. For example the Transactions entity requires a project id
condition and a time range condition.

HAVING
======

Works like the WHERE clause but it is applied after the aggregations declared
in the SELECT clause. So we can apply conditions on the result of an aggregation
function here.

ORDER BY
========

Specify the expression(s) to order the result set on.

LIMIT BY/LIMIT/OFFSET
=====================

Pretty self explanatory, they take integers and set the corresponding
values in the Clickhouse query. If a query doesn't specify the limit or
offset, they will be defaulted to 1000 and 0 respectively.

GRANULARITY
===========

An integer representing the granularity to group time based results.

TOTALS
======

If set to True, the response from Snuba will have a ``"totals"`` key that
contains the total values across all the selected rows.

SAMPLE
======

If a sampling rate isn't provided by a node in the ``MATCH`` clause, then it
can be specified here. In this case, Snuba will assign the sample right to
one of the nodes in the query. A sample can be either a float between 0 and
1, representing a percentage of rows to sample.

Or it can be an integer greater 1 which represents the number of rows to sample.
