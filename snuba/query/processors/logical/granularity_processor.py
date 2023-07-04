from typing import Mapping, NamedTuple, Optional, Set

from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.exceptions import InvalidGranularityException
from snuba.query.expressions import Column as ColumnExp
from snuba.query.expressions import Expression
from snuba.query.expressions import Literal as LiteralExp
from snuba.query.logical import Query
from snuba.query.matchers import (
    Any,
    AnyExpression,
    Column,
    FunctionCall,
    Literal,
    Or,
    Param,
    String,
)
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings

#: Granularities for which a materialized view exist, in ascending order
GRANULARITIES_AVAILABLE = (10, 60, 60 * 60, 24 * 60 * 60)
DEFAULT_GRANULARITY_RAW = 60


class GranularityProcessor(LogicalQueryProcessor):
    """Use the granularity set on the query to filter on the granularity column"""

    def __get_granularity(self, query: Query) -> int:
        """Find the best fitting granularity for this query"""
        requested_granularity = query.get_granularity()
        expression = query.get_condition()
        granularity_matches = self.__find_granularity_in_expression(expression)

        # If not granularity was provided in clause and condition, then provide a default
        if requested_granularity is None and granularity_matches is None:
            return DEFAULT_GRANULARITY_RAW

        # If multiple granularities were provided within the clause and/or condition, then raise an error
        if granularity_matches:
            if len(granularity_matches) > 1 or requested_granularity:
                raise InvalidGranularityException(
                    "Multiple granularities is not supported."
                )

        # Depending on where the granularity was provided (clause or condition), select the appropriate one
        if (
            requested_granularity
            and requested_granularity > 0
            and granularity_matches is None
        ):
            selected_granularity = requested_granularity
        elif requested_granularity is None and granularity_matches:
            selected_granularity = next(iter(granularity_matches))

        for granularity in reversed(GRANULARITIES_AVAILABLE):
            if (selected_granularity % granularity) == 0:
                return granularity
        raise InvalidGranularityException(
            f"Granularity must be multiple of one of {GRANULARITIES_AVAILABLE}"
        )

    def __find_granularity_in_expression(
        self, expression: Optional[Expression]
    ) -> Optional[Set[int]]:
        match = FunctionCall(
            String(ConditionFunctions.EQ),
            (
                Column(column_name=String("granularity")),
                Literal(value=Param("granularity", Any(int))),
            ),
        ).match(expression)
        if match is not None:
            return {match.integer("granularity")}

        match = FunctionCall(
            Param(
                "operator",
                Or([String(BooleanFunctions.AND), String(BooleanFunctions.OR)]),
            ),
            (Param("lhs", AnyExpression()), Param("rhs", AnyExpression())),
        ).match(expression)

        if match is not None:
            lhs_objects = self.__find_granularity_in_expression(match.expression("lhs"))
            rhs_objects = self.__find_granularity_in_expression(match.expression("rhs"))
            if lhs_objects is None:
                return rhs_objects
            elif rhs_objects is None:
                return lhs_objects
            else:
                return lhs_objects.union(rhs_objects)

        return None

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        granularity = self.__get_granularity(query)
        query.add_condition_to_ast(
            binary_condition(
                ConditionFunctions.EQ,
                ColumnExp(None, None, "granularity"),
                LiteralExp(None, granularity),
            )
        )
        print(query.get_condition())


class GranularityMapping(NamedTuple):
    raw: int
    enum_value: int


PERFORMANCE_GRANULARITIES: Mapping[int, int] = {
    60: 1,
    3600: 2,
    86400: 3,
}
DEFAULT_MAPPED_GRANULARITY_ENUM = 1


class MappedGranularityProcessor(LogicalQueryProcessor):
    """
    Use the granularity set on the query to filter on the granularity column,
    supporting generic-metrics style enum mapping (e.g. input granularity of 60s
    is mapped to the enum granularity of 1)
    """

    def __init__(
        self,
        accepted_granularities: Mapping[int, int],
        default_granularity: int,
    ):
        """
        Constructs a new MappedGranularityProcessor

        :param accepted_granularities: a Mapping of raw to enumerated-value granularities
            where the key is the granularity we expect from user input and the value is
            the granularity we expect to see in the table/query
        :param default_granularity: the default granularity value (as seen by the table)
            to use in the query if the user does not supply one
        """
        accepted_granularities_processed = [
            GranularityMapping(k, v) for (k, v) in accepted_granularities.items()
        ]
        self._accepted_granularities = sorted(
            accepted_granularities_processed,
            key=lambda mapping: mapping.raw,
            reverse=True,
        )
        self._available_granularities_values = [
            mapping.raw for mapping in self._accepted_granularities
        ]
        self._default_granularity_enum = default_granularity

    def __get_granularity(self, query: Query) -> int:
        """Find the best fitting granularity for this query"""
        requested_granularity = query.get_granularity()
        if requested_granularity is None:
            return self._default_granularity_enum
        elif requested_granularity > 0:
            for mapping in self._accepted_granularities:
                if requested_granularity % mapping.raw == 0:
                    return mapping.enum_value

        raise InvalidGranularityException(
            f"Granularity must be multiple of one of {self._available_granularities_values}"
        )

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        granularity = self.__get_granularity(query)
        query.add_condition_to_ast(
            binary_condition(
                ConditionFunctions.EQ,
                ColumnExp(None, None, "granularity"),
                LiteralExp(None, granularity),
            )
        )
