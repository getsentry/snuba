import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query, SelectedExpression
from snuba.query.matchers import (
    Column as ColumnMatch,
    String as StringMatch,
    MatchResult,
    Param,
)
from snuba.query.processors.match_replace_processor import MatchReplaceProcessor
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    (
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
        "nullIf(column1, '')",
    ),
    (
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
        "(nullIf(column1, '') AS some_alias)",
    ),
]


@pytest.mark.parametrize(
    "unprocessed, expected, formatted", test_data,
)
def test_match_replace_format_expressions(
    unprocessed: Query, expected: Query, formatted: str
) -> None:
    def transform(match: MatchResult, exp: Expression) -> Expression:
        assert isinstance(exp, Column)  # mypy
        return FunctionCall(
            None, "nullIf", (Column(None, None, exp.column_name), Literal(None, ""),)
        )

    processor = MatchReplaceProcessor(
        Param("column", ColumnMatch(None, None, StringMatch("column1"))), transform,
    )

    processor.process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    print(unprocessed.get_selected_columns_from_ast()[0].expression)

    ret = unprocessed.get_selected_columns_from_ast()[0].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == formatted
