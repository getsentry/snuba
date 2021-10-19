from dataclasses import replace
from typing import Callable

from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.matchers import Pattern, MatchResult, TMatchedType
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class PatternReplacer(QueryProcessor):
    """
    Define a processor that matches specific expressions and replaces them
    """

    def __init__(
        self,
        matcher: Pattern[TMatchedType],
        transformation_fn: Callable[[MatchResult, Expression], Expression],
    ) -> None:
        self.__matcher = matcher
        self.__transformation_fn = transformation_fn

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def apply_matcher(expression: Expression) -> Expression:
            result = self.__matcher.match(expression)
            if result is not None:
                ret = self.__transformation_fn(result, expression)
                return replace(ret, alias=expression.alias)

            return expression

        query.transform_expressions(apply_matcher)
