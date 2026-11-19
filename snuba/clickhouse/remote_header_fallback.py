"""
Workaround for distributed queries that fail while ClickHouse nodes run mixed
versions during a rolling upgrade (INC-2695,
https://github.com/ClickHouse/ClickHouse/issues/113250).

From 26.5, the analyzer backquotes some names in the block headers that storage
nodes send back to the query node. A Nested sub-column is written as
``__table1.`tags.key``` where older versions write ``__table1.tags.key``. The
query node matches those headers by name, so when the shard returns an
intermediate stage (partial aggregates, sorted rows) whose header mentions such
a column, the query fails with NOT_FOUND_COLUMN_IN_BLOCK. This happens in both
directions.

The shards only send that kind of header when they run part of the
aggregation, sort, or limit. When the shard runs the whole query and returns a
plain projection, the header holds the projection's aliases and matches. So the
fallback rewrites::

    SELECT <exprs> FROM t PREWHERE <p> WHERE <w> GROUP BY ... ORDER BY ... LIMIT ...

into::

    SELECT <exprs over inner aliases>
    FROM (SELECT <referenced columns AS _snuba_inner_N> FROM t PREWHERE <p> WHERE <w>)
    GROUP BY ... ORDER BY ... LIMIT ...

Filters still run on the shards. Aggregation, sorting and limits move to the
query node, which costs more network and query-node memory, so this is only
used to retry a query that already failed this way.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import replace

from snuba.clickhouse.error_codes import ErrorCodes
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.query import Query
from snuba.query import LimitBy, OrderBy, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, DangerousRawSQL, Expression, FunctionCall

INNER_ALIAS_PREFIX = "_snuba_inner_"

_HEADER_MISMATCH_RE = re.compile(
    r"Not found column (?P<missing>.+?) in block\. "
    r"There are only columns: (?P<available>.+?): While executing Remote",
    re.DOTALL,
)


def is_remote_header_mismatch(error: ClickhouseError) -> bool:
    """
    True only for the mixed-version quoting mismatch: the query node expected
    a remote header column that the shard sent with different backquoting.
    """
    if error.code != ErrorCodes.NOT_FOUND_COLUMN_IN_BLOCK:
        return False
    match = _HEADER_MISMATCH_RE.search(error.message or "")
    if match is None:
        return False
    missing, available = match.group("missing"), match.group("available")
    if "`" not in missing and "`" not in available:
        return False
    return missing.replace("`", "") in available.replace("`", "")


def _contains(expressions: Iterable[Expression], predicate: type | str) -> bool:
    for expression in expressions:
        for node in expression:
            if isinstance(predicate, str):
                if isinstance(node, FunctionCall) and node.function_name == predicate:
                    return True
            elif isinstance(node, predicate):
                return True
    return False


def wrap_in_projection_subquery(
    query: Query | CompositeQuery[Table],
) -> CompositeQuery[Table] | None:
    """
    Returns the rewritten query, or None if this query shape isn't supported,
    in which case the caller should surface the original error.
    """
    if not isinstance(query, Query) or query.is_delete():
        return None

    conditions = [c for c in (query.get_prewhere_ast(), query.get_condition()) if c is not None]
    having = query.get_having()
    limitby = query.get_limitby()
    outer_expressions: list[Expression] = [
        *(s.expression for s in query.get_selected_columns()),
        *query.get_groupby(),
        *([having] if having is not None else []),
        *(o.expression for o in query.get_orderby()),
        *(limitby.columns if limitby else []),
    ]

    # ARRAY JOIN, or arrayJoin() inside a filter, changes the row set before
    # WHERE runs, so moving WHERE into the inner query would change results.
    # Raw SQL can't be rewritten safely.
    if query.get_arrayjoin():
        return None
    if _contains(conditions, "arrayJoin"):
        return None
    if _contains(outer_expressions, DangerousRawSQL):
        return None

    inner_aliases: dict[tuple[str | None, str], str] = {}
    for expression in outer_expressions:
        for node in expression:
            if isinstance(node, Column):
                key = (node.table_name, node.column_name)
                if key not in inner_aliases:
                    inner_aliases[key] = f"{INNER_ALIAS_PREFIX}{len(inner_aliases)}"
    if not inner_aliases:
        return None

    inner = Query(
        from_clause=query.get_from_clause(),
        selected_columns=[
            SelectedExpression(
                name=alias,
                expression=Column(alias=alias, table_name=table_name, column_name=column_name),
            )
            for (table_name, column_name), alias in inner_aliases.items()
        ],
        prewhere=query.get_prewhere_ast(),
        condition=query.get_condition(),
    )

    def to_inner(node: Expression) -> Expression:
        if isinstance(node, Column):
            return Column(
                alias=node.alias,
                table_name=None,
                column_name=inner_aliases[(node.table_name, node.column_name)],
            )
        return node

    return CompositeQuery(
        from_clause=inner,
        selected_columns=[
            replace(s, expression=s.expression.transform(to_inner))
            for s in query.get_selected_columns()
        ],
        groupby=[g.transform(to_inner) for g in query.get_groupby()],
        having=having.transform(to_inner) if having is not None else None,
        order_by=[
            OrderBy(direction=o.direction, expression=o.expression.transform(to_inner))
            for o in query.get_orderby()
        ],
        limitby=(
            LimitBy(limit=limitby.limit, columns=[c.transform(to_inner) for c in limitby.columns])
            if limitby
            else None
        ),
        limit=query.get_limit(),
        offset=query.get_offset(),
        totals=query.has_totals(),
        granularity=query.get_granularity(),
    )
