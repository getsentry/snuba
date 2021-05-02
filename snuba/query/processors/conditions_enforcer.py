import logging
from typing import Mapping

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Expression, Query
from snuba.datasets.storage import condition_checker
from snuba.query.conditions import (
    ConditionFunctions,
    condition_pattern,
    get_first_level_and_conditions,
    set_condition_pattern,
)
from snuba.query.expressions import Column
from snuba.query.matchers import Any
from snuba.query.matchers import Column as ColumnPattern
from snuba.query.matchers import Literal as LiteralPattern
from snuba.query.matchers import Or, Param
from snuba.request.request_settings import RequestSettings
from snuba.state import get_config

logger = logging.getLogger(__name__)

CONDITION_PATTERN = Or(
    [
        condition_pattern(
            {ConditionFunctions.EQ},
            Param("lhs", ColumnPattern(None, Any(str))),
            LiteralPattern(Any(int)),
            commutative=True,
        ),
        set_condition_pattern[ConditionFunctions.IN],
    ],
)


def _check_int_set(expression: Expression, column_name: str) -> bool:
    match = CONDITION_PATTERN.match(expression)
    if match is not None:
        lhs = match.expression("lhs")
        if isinstance(lhs, Column) and lhs.column_name == column_name:
            return True
    return False


def enforce_project_id(expression: Expression) -> bool:
    return _check_int_set(expression, "project_id")


def enforce_org_id(expression: Expression) -> bool:
    return _check_int_set(expression, "org_id")


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

    def __init__(self, condition_checkers: Mapping[str, condition_checker]) -> None:
        self.__condition_checkers = condition_checkers

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        missing_checkers = {
            id: checker for id, checker in self.__condition_checkers.items()
        }

        def inspect_expression(condition: Expression) -> None:
            top_level = get_first_level_and_conditions(condition)
            for condition in top_level:
                for checker_id, checker in self.__condition_checkers.items():
                    if checker_id in missing_checkers:
                        if checker(condition):
                            del missing_checkers[checker_id]

        condition = query.get_condition()
        if condition is not None:
            inspect_expression(condition)

        prewhere = query.get_prewhere_ast()
        if prewhere is not None:
            inspect_expression(prewhere)

        if get_config("mandatory_condition_enforce", 0):
            assert (
                not missing_checkers
            ), f"Missing mandatory columns in query. Missing {missing_checkers.keys()}"
        else:
            if missing_checkers:
                logger.error(
                    "Query is missing mandatory columns",
                    extra={"missing_checkers": missing_checkers.keys()},
                )
