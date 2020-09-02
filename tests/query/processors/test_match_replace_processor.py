import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query, SelectedExpression
from snuba.query.matchers import (
    Column as ColumnMatch,
    String as StringMatch,
    MatchResult,
    Param,
)
from snuba.query.processors.pattern_replacer import PatternReplacer
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    pytest.param(
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(name=None, expression=Column(None, None, "column1")),
                SelectedExpression(name=None, expression=Column(None, None, "column2")),
            ],
        ),
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    name=None,
                    expression=FunctionCall(
                        None,
                        "nullIf",
                        (Column(None, None, "column1"), Literal(None, "")),
                    ),
                ),
                SelectedExpression(name=None, expression=Column(None, None, "column2")),
            ],
        ),
        id="replace unaliased column",
    ),
    pytest.param(
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    name=None, expression=Column("some_alias", None, "column1")
                ),
                SelectedExpression(name=None, expression=Column(None, None, "column2")),
            ],
        ),
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    name=None,
                    expression=FunctionCall(
                        "some_alias",
                        "nullIf",
                        (Column(None, None, "column1"), Literal(None, "")),
                    ),
                ),
                SelectedExpression(name=None, expression=Column(None, None, "column2")),
            ],
        ),
        id="replace aliased column",
    ),
]


@pytest.mark.parametrize(
    "unprocessed, expected", test_data,
)
def test_match_replace_format_expressions(unprocessed: Query, expected: Query) -> None:
    def transform(match: MatchResult, exp: Expression) -> Expression:
        assert isinstance(exp, Column)  # mypy
        return FunctionCall(
            None, "nullIf", (Column(None, None, exp.column_name), Literal(None, ""),)
        )

    processor = PatternReplacer(
        Param("column", ColumnMatch(None, None, StringMatch("column1"))), transform,
    )

    processor.process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )
