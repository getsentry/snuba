from abc import abstractmethod
from typing import List, Mapping, NamedTuple, Optional

from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.exceptions import InvalidGranularityException
from snuba.query.expressions import Column as ColumnExp
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExp
from snuba.query.expressions import Literal as LiteralExp
from snuba.query.logical import Query
from snuba.query.matchers import (
    Any,
    AnyExpression,
    Column,
    FunctionCall,
    Integer,
    Literal,
    MatchResult,
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
    @abstractmethod
    def get_and_process_granularities(self, query: Query) -> None:
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

    def find_granularities_in_expression(
        self, expression: Optional[Expression]
    ) -> List[MatchResult]:
        """
        Finds all granularity conditions in an expression. Returns List[Tuple[MatchResult, int]]
        where [0] is the matched condition and [1] is highest common available granularity multiple
        """
        matches: List[MatchResult] = []
        match = FunctionCall(
            String(ConditionFunctions.EQ),
            (
                Column(column_name=String("granularity")),
                Literal(value=Param("granularity", Any(int))),
            ),
        ).match(expression)
        if match is not None:
            matches.append(match)

        match = FunctionCall(
            Param(
                "operator",
                Or([String(BooleanFunctions.AND), String(BooleanFunctions.OR)]),
            ),
            (Param("lhs", AnyExpression()), Param("rhs", AnyExpression())),
        ).match(expression)

        if match is not None:
            lhs_granularity = self.find_granularities_in_expression(
                match.expression("lhs")
            )
            rhs_granularity = self.find_granularities_in_expression(
                match.expression("rhs")
            )
            matches.extend(rhs_granularity)
            matches.extend(lhs_granularity)
        return matches

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
        self, query: Query, match: MatchResult, selected_granularity: int
    ) -> None:
        # TODO: fix this
        def process_condition(exp: Expression) -> Expression:
            result = FunctionCall(
                String(ConditionFunctions.EQ),
                (
                    Column(column_name=String("granularity")),
                    Literal(
                        value=Param(
                            "granularity", Integer(match.integer("granularity"))
                        )
                    ),
                ),
            ).match(exp)
            if result is not None:
                assert isinstance(exp, FunctionCallExp)
                return FunctionCallExp(
                    exp.alias,
                    exp.function_name,
                    (exp.parameters[0], LiteralExp(None, selected_granularity)),
                )

            return exp

        condition = query.get_condition()
        if condition:
            query.set_ast_condition(condition.transform(process_condition))


class GranularityProcessor(BaseGranularityProcessor):
    """
    A granularity processor which finds the granularity in the query,
    validates/transforms its value according to GRANULARITIES_AVAILABLE, and
    transforms the conditions appropriately to reflect this change."""

    def get_highest_common_available_granularity_multiple(
        self,
        requested_granularity: int,
    ) -> int:
        for granularity in reversed(GRANULARITIES_AVAILABLE):
            if requested_granularity > 0 and (requested_granularity % granularity) == 0:
                return granularity
        raise InvalidGranularityException(
            f"Granularity must be multiple of one of {GRANULARITIES_AVAILABLE}"
        )

    def get_and_process_granularities(self, query: Query) -> None:
        requested_granularity = query.get_granularity()
        expression = query.get_condition()
        granularities_in_condition = self.find_granularities_in_expression(expression)

        # If not granularity was provided in clause and condition, then provide a single default
        if requested_granularity is None and not granularities_in_condition:
            self.add_granularity_condition(query, DEFAULT_GRANULARITY_RAW)
            return

        # If granularities were provided within both the GRANULARITY clause and WHERE clause, then raise an error
        if granularities_in_condition and requested_granularity:
            raise InvalidGranularityException(
                "Granularities cannot be specified in both the GRANULARITY clause and WHERE clause."
            )

        # 1. Identifies where the granularity was provided (GRANULARITY clause vs WHERE clause)
        # 2. Gets the highest common multiple of GRANULARITIES_AVAILABLE
        # 3. Process the query
        #   a. If granularity was found in GRANULARITY clause, simply just add a new condition.
        #   b. If found in WHERE clause, replace the old condition with the new one.
        if (
            requested_granularity
            and requested_granularity > 0
            and not granularities_in_condition
        ):
            selected_granularity = (
                self.get_highest_common_available_granularity_multiple(
                    requested_granularity
                )
            )
            self.add_granularity_condition(query, selected_granularity)
        elif requested_granularity is None and len(granularities_in_condition) > 0:
            for match in granularities_in_condition:
                selected_granularity = (
                    self.get_highest_common_available_granularity_multiple(
                        match.integer("granularity")
                    )
                )
                self.replace_granularity_condition(
                    query,
                    match,
                    selected_granularity,
                )
        else:
            raise InvalidGranularityException(
                "Could not select granularity from either clause or condition."
            )

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        self.get_and_process_granularities(query)


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
    A mapped granularity processor which finds the granularity in the query,
    validates/transforms its value according to the generic-metrics style
    enum mapping (e.g. input granularity of 60s is mapped to the enum
    granularity of 1), and transforms the conditions appropriately to reflect
    this change.
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
        requested_granularity: int,
    ) -> int:
        # If the requested granularity is already mapped to the enum, then just return the value.
        min_enum_granularity = min(
            [mapping.enum_value for mapping in self._accepted_granularities]
        )
        max_enum_granularity = max(
            [mapping.enum_value for mapping in self._accepted_granularities]
        )
        if min_enum_granularity <= requested_granularity <= max_enum_granularity:
            return requested_granularity

        # If the requested granularity is not mapped, find it's correct mapping
        for mapping in self._accepted_granularities:
            if requested_granularity > 0 and requested_granularity % mapping.raw == 0:
                return mapping.enum_value
        raise InvalidGranularityException(
            f"Granularity must be multiple of one of {self._available_granularities_values}"
        )

    def get_and_process_granularities(self, query: Query) -> None:
        """Find the best fitting granularity for this query"""
        requested_granularity = query.get_granularity()
        expression = query.get_condition()
        granularities_in_condition = self.find_granularities_in_expression(expression)

        # If not granularity was provided in clause and condition, then provide a default
        if requested_granularity is None and not granularities_in_condition:
            self.add_granularity_condition(query, self._default_granularity_enum)
            return

        # If multiple granularities were provided within the clause and/or condition, then raise an error
        if len(granularities_in_condition) > 0 and requested_granularity:
            raise InvalidGranularityException(
                "Granularities cannot be specified in both the GRANULARITY clause and WHERE clause."
            )

        # 1. Identifies where the granularity was provided (GRANULARITY clause vs WHERE clause)
        # 2. Gets the highest common multiple of GRANULARITIES_AVAILABLE
        # 3. Process the query
        #   a. If granularity was found in GRANULARITY clause, simple just add a condition.
        #   b. If found in WHERE clause, replace the old condition and add a new one.
        if (
            requested_granularity
            and requested_granularity > 0
            and not granularities_in_condition
        ):
            selected_granularity = (
                self.get_highest_common_available_granularity_multiple(
                    requested_granularity
                )
            )
            self.add_granularity_condition(query, selected_granularity)
        elif requested_granularity is None and len(granularities_in_condition) > 0:
            for match in granularities_in_condition:
                selected_granularity = (
                    self.get_highest_common_available_granularity_multiple(
                        match.integer("granularity")
                    )
                )
                self.replace_granularity_condition(query, match, selected_granularity)
        else:
            raise InvalidGranularityException(
                "Could not select granularity from either clause or condition."
            )

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        self.get_and_process_granularities(query)
