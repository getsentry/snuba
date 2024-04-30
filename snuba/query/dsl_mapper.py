from typing import Callable

from snuba.query.dsl import and_cond, binary_condition, equals, in_cond, or_cond
from snuba.query.matchers import (
    # Any,
    # Column,
    FunctionCall as FunctionCallMatch,
    Pattern,
    # Literal,
    # Or,
    # Param,
    # Pattern,
    String as StringMatch,
)
from snuba.query.conditions import get_first_level_and_conditions
from snuba.query.expressions import Expression, FunctionCall


MapFn = Callable[[Expression], str]


class MapperRegistry:
    def __init__(self) -> None:
        self._mappers: dict[Pattern, MapFn] = {}

    def register(self, matcher: Pattern, mapper: MapFn) -> bool:
        self._mappers[matcher] = mapper
        return True  # TODO: check for overriding values?

    def ast_repr(self, exp: Expression) -> str:
        for matcher, mapper in self._mappers.items():
            if matcher.match(exp):
                return mapper(exp)
        return str(exp)


and_cond_match = FunctionCallMatch(
    StringMatch("and"),
)


def and_cond_repr(exp: Expression) -> str:
    assert isinstance(exp, FunctionCall)
    conditions = get_first_level_and_conditions(exp)
    parameters = ", ".join([str(arg) for arg in conditions])
    return f"and_cond({parameters})"


registry = MapperRegistry()
registry.register(and_cond_match, and_cond_repr)
