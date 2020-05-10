from abc import ABC, abstractmethod
from typing import Any, Generic, Callable, Mapping, Optional, TypeVar, Tuple

from snuba.query.expressions import Expression, ExpressionVisitor

TExpIn = TypeVar("TExpIn", bound=Expression)
TExpOut = TypeVar("TExpOut")


class SimpleExpressionMapper(ABC, Generic[TExpIn, TExpOut]):
    @abstractmethod
    def attemptMap(self, expression: TExpIn) -> Optional[TExpOut]:
        raise NotImplementedError


class StructuredExpressionMapper(ABC, Generic[TExpIn, TExpOut]):
    @abstractmethod
    def attemptMap(
        self, expression: TExpIn, children_translator: ExpressionVisitor[TExpOut]
    ) -> Optional[TExpOut]:
        raise NotImplementedError


ExpressionMatcher = Callable[
    [TExpIn], Tuple[bool, Mapping[str, Any], Mapping[str, TExpIn]]
]
StructuredRuleExecutor = Callable[
    [TExpIn, Mapping[str, Any], Mapping[str, TExpOut]], TExpOut,
]


class StructuredMatchingMappingRule(
    StructuredExpressionMapper[TExpIn, TExpOut], Generic[TExpIn, TExpOut]
):
    def __init__(
        self,
        matcher: ExpressionMatcher[TExpIn],
        executor: StructuredRuleExecutor[TExpIn, TExpOut],
    ) -> None:
        self.__matcher = matcher
        self.__executor = executor

    def attemptMap(
        self, expression: TExpIn, children_translator: ExpressionVisitor[TExpOut]
    ) -> Optional[TExpOut]:
        accepted_expression, parameters, to_translate = self.__matcher(expression)
        if not accepted_expression:
            return None
        translated_params = {
            param: exp.accept(children_translator)
            for param, exp in to_translate.items()
        }
        return self.__executor(expression, parameters, translated_params)
