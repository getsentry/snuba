import pytest
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import MatchResult, Param
from snuba.query.matchers import String as StringMatch
from snuba.query.processors.pattern_replacer import PatternReplacer
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    pytest.param(
        Query(
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(name=None, expression=Column(None, None, "column1")),
                SelectedExpression(name=None, expression=Column(None, None, "column2")),
            ],
        ),
        Query(
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    name=None, expression=Column("some_alias", None, "column1")
                ),
                SelectedExpression(name=None, expression=Column(None, None, "column2")),
            ],
        ),
        Query(
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
def test_pattern_replacer_format_expressions(
    unprocessed: Query, expected: Query
) -> None:
    def transform(match: MatchResult, exp: Expression) -> Expression:
        assert isinstance(exp, Column)  # mypy
        return FunctionCall(
            None, "nullIf", (Column(None, None, exp.column_name), Literal(None, ""),)
        )

    PatternReplacer(
        Param("column", ColumnMatch(None, StringMatch("column1"))), transform,
    ).process_query(unprocessed, HTTPRequestSettings())
    assert expected.get_selected_columns() == unprocessed.get_selected_columns()
