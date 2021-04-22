from snuba.query.expressions import Column
from snuba.query.matchers import AnyExpression
from snuba.query.conditions import (
    FUNCTION_TO_OPERATOR,
    condition_pattern,
    get_first_level_and_conditions,
)
from typing import Set

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Expression, Query
from snuba.request.request_settings import RequestSettings


class MandatoryConditionEnforcer(QueryProcessor):
    """
    Ensures the query contains conditions on aa specific set of columns
    as top level conditions.

    This is supposed to be a failsafe mechanism to ensure the query
    processing pipeline does not drop conditions that are essential for
    safety (like conditions on project_id or timestamp).

    If this processor fails the query is supposed to fail as in that case
    it would be better to make a query fail that accidentally over
    exposing data for missing conditions.
    """

    def __init__(self, required_columns: Set[str]) -> None:
        self.__required_columns = required_columns

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        found_cols = set()

        pattern = condition_pattern(
            set([operator for operator in FUNCTION_TO_OPERATOR]),
            AnyExpression(),
            AnyExpression(),
            commutative=True,
        )

        def inspect_expression(condition: Expression) -> None:
            top_level = get_first_level_and_conditions(condition)
            for t in top_level:
                if pattern.match(t):
                    for e in t:
                        if (
                            isinstance(e, Column)
                            and e.column_name in self.__required_columns
                        ):
                            found_cols.add(e.column_name)

        condition = query.get_condition()
        if condition is not None:
            inspect_expression(condition)

        prewhere = query.get_prewhere_ast()
        if prewhere is not None:
            inspect_expression(prewhere)

        missing_cols = self.__required_columns - found_cols
        assert not missing_cols, f"Missing mandatory columns in query {missing_cols}"
