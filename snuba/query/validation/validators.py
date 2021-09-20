from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Set

from snuba.query import Query
from snuba.query.conditions import (
    ConditionFunctions,
    build_match,
    get_first_level_and_conditions,
)
from snuba.query.exceptions import InvalidExpressionException, InvalidQueryException


class QueryValidator(ABC):
    """
    Contains validation logic that requires the entire query. An entity has one or more
    of these validators that it adds contextual information too.
    """

    @abstractmethod
    def validate(self, query: Query, alias: Optional[str] = None,) -> None:
        """
        Validate that the query is correct. If the query is not valid, raise an
        Exception, otherwise return None. If the entity that calls this is part
        of a join query, the alias will be populated with the entity's alias.

        :param query: The query to validate.
        :type query: Query
        :param alias: The alias of the entity in a JOIN query.
        :type alias: Optional[str]
        :raises InvalidQueryException: [description]
        """
        raise NotImplementedError


class EntityRequiredColumnValidator(QueryValidator):
    def __init__(self, required_filter_columns: Set[str]) -> None:
        self.required_columns = required_filter_columns

    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        condition = query.get_condition()
        top_level = get_first_level_and_conditions(condition) if condition else []

        missing = set()
        if self.required_columns:
            for col in self.required_columns:
                match = build_match(col, [ConditionFunctions.EQ], int, alias)
                found = any(match.match(cond) for cond in top_level)
                if not found:
                    missing.add(col)

        if missing:
            raise InvalidQueryException(
                f"missing required conditions for {', '.join(missing)}"
            )


class NoTimeBasedConditionValidator(QueryValidator):
    """
    For some logic (e.g. subscriptions) we want to make sure that there are no time conditions
    on the query, so we can add conditions and ensure a certain time range is being queried.
    This validator will scan the query for any top level conditions on the specified time
    column and ensure there are no conditions.
    """

    def __init__(self, required_time_column: str) -> None:
        self.required_time_column = required_time_column
        self.match = build_match(
            required_time_column,
            [
                ConditionFunctions.EQ,
                ConditionFunctions.LT,
                ConditionFunctions.LTE,
                ConditionFunctions.GT,
                ConditionFunctions.GTE,
            ],
            datetime,
        )

    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        condition = query.get_condition()
        top_level = get_first_level_and_conditions(condition) if condition else []
        for cond in top_level:
            if self.match.match(cond):
                raise InvalidExpressionException(
                    cond,
                    f"Cannot have existing conditions on time field {self.required_time_column}",
                    report=False,
                )


class SubscriptionAllowedClausesValidator(QueryValidator):
    """
    Subscriptions expect a very specific query structure. This will ensure that only the allowed
    clauses are being used in the query, and that those clauses are in the correct structure.
    """

    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        selected = query.get_selected_columns()

        if len(selected) > 2:
            # In datasets like `sessions` dataset, we might also want to return the total count
            # along with the aggregate value
            raise InvalidQueryException(
                "a maximum of two aggregations in the select are allowed"
            )

        disallowed = ["groupby", "having", "orderby"]
        for field in disallowed:
            if getattr(query, f"get_{field}")():
                raise InvalidQueryException(
                    f"invalid clause {field} in subscription query"
                )
