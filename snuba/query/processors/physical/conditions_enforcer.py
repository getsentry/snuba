import logging
from typing import Sequence

from snuba.clickhouse.query import Expression, Query
from snuba.query.conditions import get_first_level_and_conditions
from snuba.query.processors.condition_checkers import ConditionChecker
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.state import get_config

logger = logging.getLogger(__name__)


class MandatoryConditionEnforcer(ClickhouseQueryProcessor):
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
            assert not missing_checkers, (
                f"Missing mandatory columns in query. Missing {missing_ids}"
            )
        else:
            if missing_checkers:
                logger.error(
                    "Query is missing mandatory columns",
                    extra={"missing_checkers": missing_ids},
                )
