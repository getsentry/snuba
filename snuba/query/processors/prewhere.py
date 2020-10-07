from typing import Iterable, Optional, Sequence, Tuple

from snuba import settings
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.accessors import get_columns_in_expression
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    combine_and_conditions,
    get_first_level_and_conditions,
)
from snuba.query.expressions import Column, FunctionCall
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
        prewhere_keys = query.get_data_source().get_prewhere_candidates()
        if not prewhere_keys:
            return

        PrewhereProcessorDelegate().process_query(
            query, max_prewhere_conditions, prewhere_keys
        )


class PrewhereProcessorDelegate:
    """
    Runs the prewhere generation algorithm on behalf of PrewhereProcessor.
    There are two implementation following a template method pattern. One
    for the AST and one for the legacy query representation so that the
    prewhere generation code does not have to be duplicated.
    """

    allowed_ast_operators = [OPERATOR_TO_FUNCTION[o] for o in ALLOWED_OPERATORS]

    def process_query(
        self, query: Query, max_prewhere_conditions: int, prewhere_keys: Sequence[str]
    ) -> None:
        # Use the condition that has the highest priority (based on the
        # position of its columns in the prewhere keys list)
        sorted_candidates = sorted(
            [
                (
                    min(
                        prewhere_keys.index(self._get_column_name(col))
                        for col in cols
                        if self._get_column_name(col) in prewhere_keys
                    ),
                    cond,
                )
                for cols, cond in self._get_prewhere_candidates(query, prewhere_keys)
            ],
            key=lambda priority_and_col: priority_and_col[0],
        )

        prewhere_conditions = [cond for _, cond in sorted_candidates][
            :max_prewhere_conditions
        ]

        if prewhere_conditions:
            self._update_conditions(query, prewhere_conditions)

    def _get_prewhere_candidates(
        self, query: Query, prewhere_keys: Sequence[str]
    ) -> Sequence[Tuple[Iterable[Column], FunctionCall]]:
        # Add any condition to PREWHERE if:
        # - It is a single top-level condition (not OR-nested), and
        # - Any of its referenced columns are in prewhere_keys
        ast_condition = query.get_condition_from_ast()
        return (
            [
                (get_columns_in_expression(cond), cond)
                for cond in get_first_level_and_conditions(ast_condition)
                if isinstance(cond, FunctionCall)
                and cond.function_name in self.allowed_ast_operators
                and any(
                    col.column_name in prewhere_keys
                    for col in get_columns_in_expression(cond)
                )
            ]
            if ast_condition is not None
            else []
        )

    def _get_column_name(self, column: Column) -> str:
        return column.column_name

    def _update_conditions(
        self, query: Query, prewhere_conditions: Sequence[FunctionCall]
    ) -> None:
        """
        Updates the query with the new pre-where conditions by removing them
        from the main condition clause and adding them to the pre-where clause.
        """
        ast_condition = query.get_condition_from_ast()
        # This should never be None at this point, but for mypy this can be None.
        assert ast_condition is not None

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
