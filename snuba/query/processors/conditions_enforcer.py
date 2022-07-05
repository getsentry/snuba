import logging
from typing import Sequence

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Expression, Query
from snuba.datasets.storage import ConditionChecker
from snuba.query.conditions import (
    ConditionFunctions,
    condition_pattern,
    get_first_level_and_conditions,
)
from snuba.query.matchers import Any
from snuba.query.matchers import Column as ColumnPattern
from snuba.query.matchers import FunctionCall as FunctionCallPattern
from snuba.query.matchers import Literal as LiteralPattern
from snuba.query.matchers import Or, Param, Pattern, String
from snuba.query.query_settings import QuerySettings
from snuba.state import get_config

logger = logging.getLogger(__name__)

EQ_CONDITION_PATTERN = condition_pattern(
    {ConditionFunctions.EQ},
    ColumnPattern(None, Param("lhs", Any(str))),
    LiteralPattern(Any(int)),
    commutative=True,
)

FULL_CONDITION_PATTERN = Or(
    [
        EQ_CONDITION_PATTERN,
        FunctionCallPattern(
            String(ConditionFunctions.IN),
            (
                ColumnPattern(None, Param("lhs", Any(str))),
                FunctionCallPattern(Or([String("tuple"), String("array")]), None),
            ),
        ),
    ],
)


def _check_expression(
    pattern: Pattern[Expression], expression: Expression, column_name: str
) -> bool:
    match = pattern.match(expression)
    return match is not None and match.optional_string("lhs") == column_name


class ProjectIdEnforcer(ConditionChecker):
    def get_id(self) -> str:
        return "project_id"

    def check(self, expression: Expression) -> bool:
        return _check_expression(FULL_CONDITION_PATTERN, expression, "project_id")


class OrgIdEnforcer(ConditionChecker):
    def __init__(self, field_name: str = "org_id") -> None:
        self.field_name = field_name

    def get_id(self) -> str:
        return self.field_name

    def check(self, expression: Expression) -> bool:
        return _check_expression(EQ_CONDITION_PATTERN, expression, self.field_name)


class MandatoryConditionEnforcer(QueryProcessor):
    """
    Ensures the query contains a set of storage defined conditions on
    specific columns and blocks the query if this is not true.

    This is supposed to be a failsafe mechanism to ensure the query
    processing pipeline does not drop conditions that are essential for
    safety (like conditions on project_id or org_id).

    If this processor fails, the query is supposed to fail as in that case
    it would be better to make a query fail that accidentally over
    exposing data for missing conditions.
    """

    def __init__(self, condition_checkers: Sequence[ConditionChecker]) -> None:
        self.__condition_checkers = condition_checkers

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        missing_checkers = {checker for checker in self.__condition_checkers}

        def inspect_expression(condition: Expression) -> None:
            top_level = get_first_level_and_conditions(condition)
            for condition in top_level:
                for checker in self.__condition_checkers:
                    if checker in missing_checkers:
                        if checker.check(condition):
                            missing_checkers.remove(checker)

        condition = query.get_condition()
        if condition is not None:
            inspect_expression(condition)

        prewhere = query.get_prewhere_ast()
        if prewhere is not None:
            inspect_expression(prewhere)

        missing_ids = {checker.get_id() for checker in missing_checkers}
        if get_config("mandatory_condition_enforce", 0):
            assert (
                not missing_checkers
            ), f"Missing mandatory columns in query. Missing {missing_ids}"
        else:
            if missing_checkers:
                logger.error(
                    "Query is missing mandatory columns",
                    extra={"missing_checkers": missing_ids},
                )
