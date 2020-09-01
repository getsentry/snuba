from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.matchers import (
    Column as ColumnMatch,
    String as StringMatch,
    MatchResult,
    Param,
)
from snuba.query.processors.pattern_replacer import PatternReplacer


def transform_user_to_nullable() -> PatternReplacer:
    def transform(match: MatchResult, exp: Expression) -> Expression:
        assert isinstance(exp, Column)  # mypy
        return FunctionCall(
            None, "nullIf", (Column(None, None, exp.column_name), Literal(None, ""),)
        )

    return PatternReplacer(
        Param("column", ColumnMatch(None, None, StringMatch("user"))), transform,
    )
