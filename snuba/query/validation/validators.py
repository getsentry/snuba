from abc import ABC, abstractmethod
from typing import Optional, Set

from snuba.query import Query
from snuba.query.conditions import (
    build_match,
    ConditionFunctions,
    get_first_level_and_conditions,
)
from snuba.query.exceptions import InvalidQueryException


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
