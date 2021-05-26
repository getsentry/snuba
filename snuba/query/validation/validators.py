from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Set

from snuba.query import Query
from snuba.query.conditions import (
    FUNCTION_TO_OPERATOR,
    ConditionFunctions,
    build_match,
    get_first_level_and_conditions,
)
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import FunctionCall, Literal


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


class OneProjectValidator(QueryValidator):
    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        match = build_match("project_id", list(FUNCTION_TO_OPERATOR.keys()), int)
        condition = query.get_condition()
        top_level = get_first_level_and_conditions(condition) if condition else []
        matched_id: Optional[int] = None
        for cond in top_level:
            result = match.match(cond)
            if result is not None:
                rhs = result.expression("rhs")
                project_id: Optional[int] = None
                if isinstance(rhs, Literal) and isinstance(rhs.value, int):
                    project_id = rhs.value
                elif isinstance(rhs, FunctionCall):
                    project_ids = set(rhs.parameters)
                    if len(project_ids) != 1:
                        raise InvalidQueryException("Must only query one project")
                    value = project_ids.pop()
                    assert isinstance(value, Literal) and isinstance(value.value, int)
                    project_id = value.value

                if matched_id is not None and matched_id != project_id:
                    raise InvalidQueryException("Must only query one project")
                else:
                    matched_id = project_id


class NoTimeBasedConditionValidator(QueryValidator):
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
        if any([self.match.match(cond) for cond in top_level]):
            raise InvalidQueryException(
                f"Cannot have existing conditions on time field {self.required_time_column}"
            )


class SubscriptionAllowedClausesValidator(QueryValidator):
    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        selected = query.get_selected_columns()
        if len(selected) != 1:
            raise InvalidQueryException("Can only have one aggregation in the select")

        disallowed = ["groupby", "having", "orderby"]
        for field in disallowed:
            if getattr(query, f"get_{field}")():
                print(field, getattr(query, f"get_{field}")())
                raise InvalidQueryException(
                    f"Invalid clause {field} in subscription query"
                )
