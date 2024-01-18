from typing import Optional, Sequence, Set

from snuba import environment, settings
from snuba.clickhouse.query import Query
from snuba.query.accessors import get_columns_in_expression
from snuba.query.conditions import (
    ConditionFunctions,
    combine_and_conditions,
    get_first_level_and_conditions,
)
from snuba.query.expressions import FunctionCall
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.utils.metrics.wrapper import MetricsWrapper

ALLOWED_OPERATORS = [
    ConditionFunctions.GT,
    ConditionFunctions.LT,
    ConditionFunctions.GTE,
    ConditionFunctions.LTE,
    ConditionFunctions.EQ,
    ConditionFunctions.NEQ,
    ConditionFunctions.IN,
    ConditionFunctions.IS_NULL,
    ConditionFunctions.IS_NOT_NULL,
    ConditionFunctions.LIKE,
]

metrics = MetricsWrapper(environment.metrics, "prewhere")


# Changing this so all of CI runs
class PrewhereProcessor(ClickhouseQueryProcessor):
    """
    Moves top level conditions into the pre-where clause according to
    the list of candidates provided by the query data source.

    In order for a condition to become a pre-where condition it has to be:
    - a single top-level condition (not in an OR statement)
    - any of its referenced columns must be in the list provided by
      the query data source.
    """

    def __init__(
        self,
        prewhere_candidates: Sequence[str],
        omit_if_final: Optional[Sequence[str]] = None,
        max_prewhere_conditions: Optional[int] = None,
    ) -> None:
        self.__prewhere_candidates = prewhere_candidates
        self.__omit_if_final = omit_if_final
        self.__max_prewhere_conditions: Optional[int] = max_prewhere_conditions

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        max_prewhere_conditions: int = (
            self.__max_prewhere_conditions or settings.MAX_PREWHERE_CONDITIONS
        )
        prewhere_keys = self.__prewhere_candidates

        # We remove the candidates that appear in a uniq or -If aggregations
        # because a query like `countIf(col=x) .. PREWHERE col=x` can make
        # the Clickhouse server crash.
        uniq_cols: Set[str] = set()
        expressions = query.get_all_expressions()
        for exp in expressions:
            if isinstance(exp, FunctionCall) and (
                exp.function_name == "uniq" or exp.function_name.endswith("If")
            ):
                columns = get_columns_in_expression(exp)
                for c in columns:
                    uniq_cols.add(c.column_name)

        for col in uniq_cols:
            if col in prewhere_keys:
                metrics.increment(
                    "uniq_col_in_prewhere_candidate",
                    tags={"column": col, "referrer": query_settings.referrer},
                )

        prewhere_keys = [key for key in prewhere_keys if key not in uniq_cols]

        # In case the query is final we cannot simply add any candidate
        # condition to the prewhere.
        # Final is applied after prewhere, so there are cases where moving
        # conditions to the prewhere could exclude from the result sets
        # rows that would be merged under the `final` condition.
        # Example, rewriting the group_id on an unmerge. If the group_id
        # is in the prewhere, final wil fail at merging the rows.
        # HACK: If query has final, do not move any condition on a column in the
        # omit_if_final list to prewhere.
        # There is a bug in ClickHouse affecting queries with FINAL and PREWHERE
        # with Low Cardinality and Nullable columns.
        # https://github.com/ClickHouse/ClickHouse/issues/16171
        if query.get_from_clause().final and self.__omit_if_final:
            prewhere_keys = [
                key for key in prewhere_keys if key not in self.__omit_if_final
            ]

        if not prewhere_keys:
            return

        ast_condition = query.get_condition()
        if ast_condition is None:
            return

        prewhere_candidates = [
            (get_columns_in_expression(cond), cond)
            for cond in get_first_level_and_conditions(ast_condition)
            if isinstance(cond, FunctionCall)
            and cond.function_name in ALLOWED_OPERATORS
            and any(
                col.column_name in prewhere_keys
                for col in get_columns_in_expression(cond)
            )
        ]
        if not prewhere_candidates:
            return

        # Use the condition that has the highest priority (based on the
        # position of its columns in the prewhere keys list)
        sorted_candidates = sorted(
            [
                (
                    min(
                        prewhere_keys.index(col.column_name)
                        for col in cols
                        if col.column_name in prewhere_keys
                    ),
                    cond,
                )
                for cols, cond in prewhere_candidates
            ],
            key=lambda priority_and_col: priority_and_col[0],
        )
        prewhere_conditions = [cond for _, cond in sorted_candidates][
            :max_prewhere_conditions
        ]

        new_conditions = [
            cond
            for cond in get_first_level_and_conditions(ast_condition)
            if cond not in prewhere_conditions
        ]

        query.set_ast_condition(
            combine_and_conditions(new_conditions) if new_conditions else None
        )
        query.set_prewhere_ast_condition(
            combine_and_conditions(prewhere_conditions) if prewhere_conditions else None
        )
