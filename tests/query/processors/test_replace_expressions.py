import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query, SelectedExpression
from snuba.query.processors.replace_expressions import transform_user_to_nullable
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    pytest.param(
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(name=None, expression=Column(None, None, "user")),
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
                        None, "nullIf", (Column(None, None, "user"), Literal(None, "")),
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
                    name=None, expression=Column("some_alias", None, "user")
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
                        (Column(None, None, "user"), Literal(None, "")),
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
def test_user_nullable_format_expressions(unprocessed: Query, expected: Query) -> None:
    transform_user_to_nullable().process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )
