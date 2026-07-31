from __future__ import annotations

from typing import Optional, Sequence

from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query
from snuba.query import SelectedExpression
from snuba.query.accessors import get_columns_in_expression
from snuba.query.conditions import (
    ConditionFunctions,
    combine_and_conditions,
    get_first_level_and_conditions,
)
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import DangerousRawSQL, Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings

# HAVING comparisons that mean "this group has at least one matching row".
# For these, adding `key IN (subquery matching the same predicate)` to the
# WHERE only removes rows belonging to groups that the HAVING would have
# dropped anyway, so it is always safe (and the HAVING is left in place).
_POSITIVE_EXISTENCE = {
    ConditionFunctions.NEQ: (0,),  # sum(pred) != 0
    ConditionFunctions.GT: (0,),  # sum(pred) > 0
    ConditionFunctions.GTE: (1,),  # sum(pred) >= 1
}


class SubqueryFilterOptimizer(ClickhouseQueryProcessor):
    """
    Rewrites per-group existence filters expressed in the HAVING clause into a
    semi-join subquery on the WHERE clause so that ClickHouse skip indexes can
    prune granules before the (expensive) GROUP BY.

    Queries that look for "replays where a specific user/attribute appears in at
    least one segment" are generated as::

        SELECT replay_id, ...
        FROM replays_local
        WHERE project_id = X AND timestamp >= A AND timestamp < B AND ...
        GROUP BY replay_id
        HAVING sum(user_id = '...') != 0

    The only selective predicate (`user_id = '...'`) lives inside the HAVING, so
    it cannot restrict the rows read: ClickHouse must scan every segment for the
    project/time window before the HAVING can run. The bloom-filter skip indexes
    on those columns are never consulted.

    This processor detects each `sum(<pred>) != 0` (and `> 0` / `>= 1`) HAVING
    condition whose predicate references only configured `filter_columns`, and
    adds::

        key_column (GLOBAL) IN (SELECT key_column FROM <table> WHERE <where> AND <pred>)

    The inner query equality lets the bloom-filter index prune granules, and the
    `key_column IN (...)` on the outer query lets the `key_column` bloom-filter
    index prune granules there too. The original HAVING is intentionally kept so
    the result is provably unchanged -- the subquery is purely an accelerator.

    Args:
        key_column: column projected by and joined on in the subquery (the
            GROUP BY key, e.g. ``replay_id``). Must have a skip index for the
            outer ``IN`` to prune.
        filter_columns: columns eligible to drive the optimization. Should be
            columns backed by a skip index (e.g. ``bloom_filter``) so the inner
            query prunes granules.
        use_global_in: emit ``globalIn`` instead of ``in``. Preferred when the
            table is Distributed so the small subquery result is computed once
            and broadcast, rather than re-evaluated per shard.
    """

    def __init__(
        self,
        key_column: str,
        filter_columns: Sequence[str],
        use_global_in: bool = True,
    ) -> None:
        self.__key_column = key_column
        self.__filter_columns = set(filter_columns)
        self.__in_function = "globalIn" if use_global_in else "in"

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        having = query.get_having()
        if having is None:
            return

        condition = query.get_condition()

        predicates = [
            pred
            for cond in get_first_level_and_conditions(having)
            if (pred := self.__match_existence_predicate(cond)) is not None
        ]
        if not predicates:
            return

        for predicate in predicates:
            subquery_condition: Expression = (
                combine_and_conditions([condition, predicate])
                if condition is not None
                else predicate
            )
            subquery = Query(
                from_clause=query.get_from_clause(),
                selected_columns=[
                    SelectedExpression(
                        self.__key_column,
                        ColumnExpr(None, None, self.__key_column),
                    )
                ],
                condition=subquery_condition,
            )
            subquery_sql = format_query(subquery).get_sql()
            query.add_condition_to_ast(
                FunctionCallExpr(
                    None,
                    self.__in_function,
                    (
                        ColumnExpr(None, None, self.__key_column),
                        DangerousRawSQL(None, f"({subquery_sql})"),
                    ),
                )
            )

    def __match_existence_predicate(self, cond: Expression) -> Optional[Expression]:
        """
        If ``cond`` is a positive-existence HAVING condition of the form
        ``sum(<pred>) <op> <literal>`` where ``<pred>`` references only
        ``filter_columns``, return ``<pred>``. Otherwise return ``None``.
        """
        if not isinstance(cond, FunctionCallExpr):
            return None
        allowed_literals = _POSITIVE_EXISTENCE.get(cond.function_name)
        if allowed_literals is None or len(cond.parameters) != 2:
            return None

        agg, rhs = cond.parameters
        if not isinstance(rhs, LiteralExpr) or rhs.value not in allowed_literals:
            return None
        if (
            not isinstance(agg, FunctionCallExpr)
            or agg.function_name != "sum"
            or len(agg.parameters) != 1
        ):
            return None

        predicate = agg.parameters[0]
        columns = get_columns_in_expression(predicate)
        if not columns or any(col.column_name not in self.__filter_columns for col in columns):
            return None

        return predicate
