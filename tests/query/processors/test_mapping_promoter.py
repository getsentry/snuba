import pytest

from snuba.clickhouse.columns import ColumnSet, Nested, Nullable, String
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.request.request_settings import HTTPRequestSettings

columns = ColumnSet(
    [
        ("promoted", Nullable(String())),
        ("tags", Nested([("key", String()), ("value", String())])),
    ]
)

test_cases = [
    (
        "not promoted",
        ClickhouseQuery(
            LogicalQuery(
                {},
                TableSource("events", columns),
                selected_columns=[
                    FunctionCall(
                        "tags[foo]",
                        "arrayValue",
                        (
                            Column(None, None, "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (Column(None, None, "tags.key"), Literal(None, "foo")),
                            ),
                        ),
                    )
                ],
            )
        ),
        ClickhouseQuery(
            LogicalQuery(
                {},
                TableSource("events", columns),
                selected_columns=[
                    FunctionCall(
                        "tags[foo]",
                        "arrayValue",
                        (
                            Column(None, None, "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (Column(None, None, "tags.key"), Literal(None, "foo")),
                            ),
                        ),
                    )
                ],
            )
        ),
    ),
    (
        "replaced with promoted col",
        ClickhouseQuery(
            LogicalQuery(
                {},
                TableSource("events", columns),
                selected_columns=[
                    FunctionCall(
                        "tags[promoted_tag]",
                        "arrayValue",
                        (
                            Column(None, "table", "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(None, "table", "tags.key"),
                                    Literal(None, "promoted_tag"),
                                ),
                            ),
                        ),
                    )
                ],
            )
        ),
        ClickhouseQuery(
            LogicalQuery(
                {},
                TableSource("events", columns),
                selected_columns=[
                    FunctionCall(
                        "tags[promoted_tag]",
                        "toString",
                        (Column(None, "table", "promoted"),),
                    )
                ],
            )
        ),
    ),
]


@pytest.mark.parametrize("name, query, expected_query", test_cases)
def test_format_expressions(
    name: str, query: ClickhouseQuery, expected_query: ClickhouseQuery
) -> None:
    MappingColumnPromoter({"tags": {"promoted_tag": "promoted"}}).process_query(
        query, HTTPRequestSettings()
    )

    assert query.get_arrayjoin_from_ast() == expected_query.get_arrayjoin_from_ast()
