from __future__ import annotations

from snuba.clickhouse.query import Expression
from snuba.query.conditions import ConditionFunctions, condition_pattern
from snuba.query.matchers import Any, Or, Param, Pattern, String
from snuba.query.matchers import Column as ColumnPattern
from snuba.query.matchers import FunctionCall as FunctionCallPattern
from snuba.query.matchers import Literal as LiteralPattern
from snuba.query.processors.condition_checkers import ConditionChecker

EQ_CONDITION_PATTERN = condition_pattern(
    {ConditionFunctions.EQ},
    ColumnPattern(None, Param("lhs", Any(str))),
    LiteralPattern(Any(int)),
    commutative=True,
)

FULL_CONDITION_PATTERN = Or(
    [
        EQ_CONDITION_PATTERN,
        FunctionCallPattern(
            String(ConditionFunctions.IN),
            (
                ColumnPattern(None, Param("lhs", Any(str))),
                FunctionCallPattern(Or([String("tuple"), String("array")]), None),
            ),
        ),
    ],
)


def _check_expression(
    pattern: Pattern[Expression], expression: Expression, column_name: str
) -> bool:
    match = pattern.match(expression)
    return match is not None and match.optional_string("lhs") == column_name


class ProjectIdEnforcer(ConditionChecker):
    def get_id(self) -> str:
        return "project_id"

    def check(self, expression: Expression) -> bool:
        return _check_expression(FULL_CONDITION_PATTERN, expression, "project_id")


class OrgIdEnforcer(ConditionChecker):
    def __init__(self, field_name: str = "org_id") -> None:
        self.field_name = field_name

    def get_id(self) -> str:
        return self.field_name

    def check(self, expression: Expression) -> bool:
        return _check_expression(EQ_CONDITION_PATTERN, expression, self.field_name)
