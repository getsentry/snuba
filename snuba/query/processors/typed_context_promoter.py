import logging
import uuid
from abc import ABC, abstractmethod
from typing import NamedTuple, Optional, Set

from snuba import environment
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Expression, Query
from snuba.clickhouse.translators.snuba.mappers import (
    KEY_MAPPING_PARAM,
    TABLE_MAPPING_PARAM,
    VALUE_COL_MAPPING_PARAM,
    mapping_pattern,
)
from snuba.query.conditions import (
    ConditionFunctions,
    binary_condition,
    condition_pattern,
)
from snuba.query.expressions import Column
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import Any, Literal, MatchResult, Param, Pattern
from snuba.request.request_settings import RequestSettings
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "typed_context")
logger = logging.getLogger(__name__)


class ContextType(ABC):
    """
    Identifies a string literal in an expression and transforms
    it into a literal of the specific type.

    This is quite coupled with TypedContextPromoter.
    """

    PARAM_NAME = "condition_literal"

    def literal_pattern(self) -> Pattern[Expression]:
        return Literal(Param(self.PARAM_NAME, Any(str)))

    def literal_replacement(self, result: MatchResult) -> Optional[Expression]:
        query_value = result.string(self.PARAM_NAME)
        try:
            return self._build_literal(query_value)
        except ValueError:
            logger.warn(
                "Invalid literal expression for context condition.",
                extra={"value": str(query_value), "expected": str(self.__class__)},
            )
            return None

    @abstractmethod
    def _build_literal(self, query_value: str) -> LiteralExpr:
        raise NotImplementedError


class UUIDContextType(ContextType):
    def _build_literal(self, query_value: str) -> LiteralExpr:
        return LiteralExpr(None, str(uuid.UUID(query_value)))


class HexIntContextType(ContextType):
    def _build_literal(self, query_value: str) -> LiteralExpr:
        return LiteralExpr(None, int(query_value, 16))


class PromotionSpec(NamedTuple):
    context_name: str
    promoted_column_name: str
    data_type: ContextType


class TypedContextPromoter(QueryProcessor):
    """
    Promotes conditions on contexts that are not of string types.

    This acts only on the WHERE clause and does not replace anything
    in the select or group by clauses. Such improvements can be done
    to further reduce the amount of data fetched.

    The conditions it promtoes are of the form:
    `arrayElement(contexts.values, indexOf(contexts.key, 'NAME')) = 'string_uuid'`
    into:
    `promoted_column = the_proper_representation_of_string_uuid`
    """

    def __init__(self, mapping_column: str, specs: Set[PromotionSpec]) -> None:
        self.__mapping_column = mapping_column

        # Generates the patterns to match upfront.
        self.__patterns = {
            spec.context_name: condition_pattern(
                {ConditionFunctions.EQ},
                mapping_pattern,
                spec.data_type.literal_pattern(),
                True,
            )
            for spec in specs
        }
        self.__specs = {spec.context_name: spec for spec in specs}

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def apply_pattern(
            exp: Expression,
            pattern: Pattern[Expression],
            data_type: ContextType,
            promoted_col_name: str,
            context_name: str,
        ) -> Optional[Expression]:
            """
            Replaces the matching condition if it matches a specific
            pattern representing a condition on a context.
            """
            result = pattern.match(exp)
            if (
                result is None
                or result.string(VALUE_COL_MAPPING_PARAM)
                != f"{self.__mapping_column}.value"
                or result.string(KEY_MAPPING_PARAM) != context_name
            ):
                return None

            replacement = data_type.literal_replacement(result)
            if replacement is None:
                return None

            return binary_condition(
                ConditionFunctions.EQ,
                Column(
                    None, result.optional_string(TABLE_MAPPING_PARAM), promoted_col_name
                ),
                replacement,
            )

        def apply_all_patterns(exp: Expression) -> Expression:
            for context_name, pattern in self.__patterns.items():
                ret = apply_pattern(
                    exp,
                    pattern,
                    self.__specs[context_name].data_type,
                    self.__specs[context_name].promoted_column_name,
                    context_name,
                )
                if ret is not None:
                    return ret

            return exp

        condition = query.get_condition_from_ast()
        if condition is None:
            return

        query.set_ast_condition(condition.transform(apply_all_patterns))
