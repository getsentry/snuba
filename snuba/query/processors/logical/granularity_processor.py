from abc import abstractmethod
from typing import Callable, Mapping, NamedTuple, Optional, Tuple

from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
    get_first_level_and_conditions,
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


class BaseGranularityProcessor(LogicalQueryProcessor):
    """"""

    @abstractmethod
    def get_granularity(self, query: Query) -> Tuple[int, Callable[[Query, int], None]]:
        raise NotImplementedError

    @abstractmethod
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_highest_common_available_granularity_multiple(
        self,
        selected_granularity: int,
    ) -> int:
        raise NotImplementedError

    def find_granularity_in_expression(
        self, expression: Optional[Expression]
    ) -> Optional[int]:
        match = FunctionCall(
            String(ConditionFunctions.EQ),
            (
                Column(column_name=String("granularity")),
                Literal(value=Param("granularity", Any(int))),
            ),
        ).match(expression)
        if match is not None:
            return match.integer("granularity")

        match = FunctionCall(
            Param(
                "operator",
                Or([String(BooleanFunctions.AND), String(BooleanFunctions.OR)]),
            ),
            (Param("lhs", AnyExpression()), Param("rhs", AnyExpression())),
        ).match(expression)

        if match is not None:
            lhs_granularity = self.find_granularity_in_expression(
                match.expression("lhs")
            )
            rhs_granularity = self.find_granularity_in_expression(
                match.expression("rhs")
            )
            if lhs_granularity is None:
                return rhs_granularity
            elif rhs_granularity is None:
                return lhs_granularity
            else:
                raise InvalidGranularityException(
                    "Multiple granularities is not supported."
                )

        return None

    def add_granularity_condition(
        self, query: Query, selected_granularity: int
    ) -> None:
        query.add_condition_to_ast(
            binary_condition(
                ConditionFunctions.EQ,
                ColumnExp(None, None, "granularity"),
                LiteralExp(None, selected_granularity),
            )
        )

    def replace_granularity_condition(
        self, query: Query, selected_granularity: int
    ) -> None:
        expression = query.get_condition()
        conditions = []
        if expression:
            for c in get_first_level_and_conditions(expression):
                match = FunctionCall(
                    String(ConditionFunctions.EQ),
                    (
                        Column(column_name=String("granularity")),
                        Literal(value=Param("granularity", Any(int))),
                    ),
                ).match(c)
                if not match:
                    conditions.append(c)
        query.set_ast_condition(combine_and_conditions(conditions))
        self.add_granularity_condition(query, selected_granularity)


class GranularityProcessor(BaseGranularityProcessor):
    """Use the granularity set on the query to filter on the granularity column"""

    def get_highest_common_available_granularity_multiple(
        self,
        selected_granularity: int,
    ) -> int:
        for granularity in reversed(GRANULARITIES_AVAILABLE):
            if (selected_granularity % granularity) == 0:
                return granularity
        raise InvalidGranularityException(
            f"Granularity must be multiple of one of {GRANULARITIES_AVAILABLE}"
        )

    def get_granularity(self, query: Query) -> Tuple[int, Callable[[Query, int], None]]:
        """
        Gets the granularity value from either the GRANULARITY clause or the WHERE clause. Raises an error if multiple granularities are provided.
        Depending on where the granularity was found, return its value along with a callable method which alters the conditions appropriately.
        """

        requested_granularity = query.get_granularity()
        expression = query.get_condition()
        granularity_in_condition = self.find_granularity_in_expression(expression)

        # If not granularity was provided in clause and condition, then provide a default
        if requested_granularity is None and granularity_in_condition is None:
            return DEFAULT_GRANULARITY_RAW, self.add_granularity_condition

        # If multiple granularities were provided within the clause and/or condition, then raise an error
        if granularity_in_condition and requested_granularity:
            raise InvalidGranularityException(
                "Multiple granularities is not supported."
            )

        # Identifies where the granularity was provided (GRANULARITY clause vs WHERE clause)
        # Gets the highest common multiple of GRANULARITIES_AVAILABLE
        # Returns this granularity along with a method which alters the conditions.
        # If granularity was found in GRANULARITY clause, simple just add a condition.
        # If found in WHERE clause, replace the old condition and add a new one.
        if (
            requested_granularity
            and requested_granularity > 0
            and granularity_in_condition is None
        ):
            selected_granularity = (
                self.get_highest_common_available_granularity_multiple(
                    requested_granularity
                )
            )
            return (selected_granularity, self.add_granularity_condition)
        elif requested_granularity is None and granularity_in_condition:
            selected_granularity = (
                self.get_highest_common_available_granularity_multiple(
                    granularity_in_condition
                )
            )
            return selected_granularity, self.replace_granularity_condition
        raise InvalidGranularityException(
            "Could not select granularity from either clause or condition."
        )

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        granularity, update_conditions_func = self.get_granularity(query)
        update_conditions_func(query, granularity)


class GranularityMapping(NamedTuple):
    raw: int
    enum_value: int


PERFORMANCE_GRANULARITIES: Mapping[int, int] = {
    60: 1,
    3600: 2,
    86400: 3,
}
DEFAULT_MAPPED_GRANULARITY_ENUM = 1


class MappedGranularityProcessor(BaseGranularityProcessor):
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

    def get_highest_common_available_granularity_multiple(
        self,
        selected_granularity: int,
    ) -> int:
        for mapping in self._accepted_granularities:
            if selected_granularity % mapping.raw == 0:
                return mapping.enum_value
        raise InvalidGranularityException(
            f"Granularity must be multiple of one of {self._available_granularities_values}"
        )

    def get_granularity(self, query: Query) -> Tuple[int, Callable[[Query, int], None]]:
        """Find the best fitting granularity for this query"""
        requested_granularity = query.get_granularity()
        expression = query.get_condition()
        granularity_in_condition = self.find_granularity_in_expression(expression)

        # If not granularity was provided in clause and condition, then provide a default
        if requested_granularity is None and granularity_in_condition is None:
            return self._default_granularity_enum, self.add_granularity_condition

        # If multiple granularities were provided within the clause and/or condition, then raise an error
        if granularity_in_condition and requested_granularity:
            raise InvalidGranularityException(
                "Multiple granularities is not supported."
            )

        # Identifies where the granularity was provided (GRANULARITY clause vs WHERE clause)
        # Gets the highest common multiple of GRANULARITIES_AVAILABLE
        # Returns this granularity along with a method which alters the conditions.
        # If granularity was found in GRANULARITY clause, simple just add a condition.
        # If found in WHERE clause, replace the old condition and add a new one.
        if (
            requested_granularity
            and requested_granularity > 0
            and granularity_in_condition is None
        ):
            selected_granularity = (
                self.get_highest_common_available_granularity_multiple(
                    requested_granularity
                )
            )
            return selected_granularity, self.add_granularity_condition
        elif requested_granularity is None and granularity_in_condition:
            selected_granularity = (
                self.get_highest_common_available_granularity_multiple(
                    granularity_in_condition
                )
            )
            return selected_granularity, self.replace_granularity_condition
        raise InvalidGranularityException(
            "Could not select granularity from either clause or condition."
        )

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        granularity, update_conditions_func = self.get_granularity(query)
        update_conditions_func(query, granularity)
