from typing import Optional, Sequence

from snuba import settings, util
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.accessors import get_columns_in_expression
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    BooleanFunctions,
    combine_conditions,
    get_first_level_conditions,
)
from snuba.query.expressions import Column, FunctionCall
from snuba.query.types import Condition
from snuba.request.request_settings import RequestSettings

ALLOWED_OPERATORS = [
    ">",
    "<",
    ">=",
    "<=",
    "=",
    "!=",
    "IN",
    "IS NULL",
    "IS NOT NULL",
    "LIKE",
]

ALLOWED_AST_OPERATORS = [OPERATOR_TO_FUNCTION[o] for o in ALLOWED_OPERATORS]


class PrewhereProcessor(QueryProcessor):
    """
    Moves top level conditions into the pre-where clause
    according to the list of candidates provided by the query data source.

    In order for a condition to become a pre-where condition it has
    to be:
    - a single top-level condition (not in an OR statement)
    - any of its referenced columns must be in the list provided to the
      constructor.
    """

    def __init__(self, max_prewhere_conditions: Optional[int] = None) -> None:
        self.__max_prewhere_conditions: Optional[int] = max_prewhere_conditions

    def process_query(self, query: Query, request_settings: RequestSettings,) -> None:
        max_prewhere_conditions: int = (
            self.__max_prewhere_conditions or settings.MAX_PREWHERE_CONDITIONS
        )
        prewhere_keys = query.get_data_source().get_prewhere_candidates()
        if not prewhere_keys:
            return
        prewhere_conditions: Sequence[Condition] = []
        # Add any condition to PREWHERE if:
        # - It is a single top-level condition (not OR-nested), and
        # - Any of its referenced columns are in prewhere_keys
        conditions = query.get_conditions()
        if not conditions:
            return
        prewhere_candidates = [
            (util.columns_in_expr(cond[0]), cond)
            for cond in conditions
            if util.is_condition(cond)
            and cond[1] in ALLOWED_OPERATORS
            and any(col in prewhere_keys for col in util.columns_in_expr(cond[0]))
        ]

        ast_condition = query.get_condition_from_ast()
        if ast_condition:
            ast_prewhere_candidates = [
                (get_columns_in_expression(cond), cond)
                for cond in get_first_level_conditions(ast_condition)
                if isinstance(cond, FunctionCall)
                and len(cond.parameters) > 0
                and cond.function_name in ALLOWED_AST_OPERATORS
                and any(
                    col.column_name in prewhere_keys
                    for col in get_columns_in_expression(cond)
                )
            ]
        else:
            ast_prewhere_candidates = []
        # Use the condition that has the highest priority (based on the
        # position of its columns in the prewhere keys list)
        prewhere_candidates = sorted(
            [
                (
                    min(
                        prewhere_keys.index(col) for col in cols if col in prewhere_keys
                    ),
                    cond,
                )
                for cols, cond in prewhere_candidates
            ],
            key=lambda priority_and_col: priority_and_col[0],
        )
        ast_prewhere_candidates = sorted(
            [
                (
                    min(
                        prewhere_keys.index(col.column_name)
                        for col in cols
                        if col.column_name in prewhere_keys
                    ),
                    cond,
                )
                for cols, cond in ast_prewhere_candidates
            ],
            key=lambda priority_and_col: priority_and_col[0],
        )

        if prewhere_candidates:
            prewhere_conditions = [cond for _, cond in prewhere_candidates][
                :max_prewhere_conditions
            ]
            query.set_conditions(
                list(filter(lambda cond: cond not in prewhere_conditions, conditions))
            )
            query.set_prewhere(prewhere_conditions)

        if ast_condition and ast_prewhere_candidates:
            ast_prewhere_conditions = [cond for _, cond in ast_prewhere_candidates][
                :max_prewhere_conditions
            ]
            top_level_conditions = get_first_level_conditions(ast_condition)
            new_conditions = list(
                filter(
                    lambda cond: cond not in ast_prewhere_conditions,
                    top_level_conditions,
                )
            )
            query.replace_ast_condition(
                combine_conditions(new_conditions, BooleanFunctions.AND)
                if new_conditions
                else None
            )
            query.replace_prewhere_ast_condition(
                combine_conditions(ast_prewhere_conditions, BooleanFunctions.AND)
                if ast_prewhere_conditions
                else None
            )
