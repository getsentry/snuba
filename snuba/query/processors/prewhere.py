from typing import Optional

from snuba import settings
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.accessors import get_columns_in_expression
from snuba.query.conditions import (
    ConditionFunctions,
    combine_and_conditions,
    get_first_level_and_conditions,
)
from snuba.query.expressions import FunctionCall
from snuba.request.request_settings import RequestSettings

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


class PrewhereProcessor(QueryProcessor):
    """
    Moves top level conditions into the pre-where clause according to
    the list of candidates provided by the query data source.

    In order for a condition to become a pre-where condition it has to be:
    - a single top-level condition (not in an OR statement)
    - any of its referenced columns must be in the list provided by
      the query data source.
    """

    def __init__(self, max_prewhere_conditions: Optional[int] = None) -> None:
        self.__max_prewhere_conditions: Optional[int] = max_prewhere_conditions

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        max_prewhere_conditions: int = (
            self.__max_prewhere_conditions or settings.MAX_PREWHERE_CONDITIONS
        )
        prewhere_keys = query.get_from_clause().get_prewhere_candidates()
        if not prewhere_keys:
            return

        ast_condition = query.get_condition_from_ast()
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
