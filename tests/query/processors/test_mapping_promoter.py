import pytest

from snuba.clickhouse.columns import ColumnSet, Nested, Nullable, String
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.mapping_promoter import (
    MappingColumnPromoter,
    PromotedColumnsSpec,
)
from snuba.request.request_settings import HTTPRequestSettings

test_cases = [
    (
        "not promoted",
        ClickhouseQuery(
            LogicalQuery(
                {},
                TableSource("events", ColumnSet([])),
                selected_columns=[
                    FunctionCall(
                        "tags[foo]",
                        "arrayValue",
                        (
                            Column(None, "tags.value", None),
                            FunctionCall(
                                None,
                                "indexOf",
                                (Column(None, "tags.key", None), Literal(None, "foo")),
                            ),
                        ),
                    )
                ],
            )
        ),
        ClickhouseQuery(
            LogicalQuery(
                {},
                TableSource("events", ColumnSet([])),
                selected_columns=[
                    FunctionCall(
                        "tags[foo]",
                        "arrayValue",
                        (
                            Column(None, "tags.value", None),
                            FunctionCall(
                                None,
                                "indexOf",
                                (Column(None, "tags.key", None), Literal(None, "foo")),
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
                TableSource("events", ColumnSet([])),
                selected_columns=[
                    FunctionCall(
                        "tags[promoted_tag]",
                        "arrayValue",
                        (
                            Column(None, "tags.value", "table"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(None, "tags.key", "table"),
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
                TableSource("events", ColumnSet([])),
                selected_columns=[
                    FunctionCall(
                        "tags[promoted_tag]",
                        "toString",
                        (Column(None, "promoted", "table"),),
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
    columns = ColumnSet(
        [
            ("promtoed", Nullable(String())),
            ("tags", Nested([("key", String()), ("value", String())])),
        ]
    )
    MappingColumnPromoter(
        columns,
        {"tags": PromotedColumnsSpec("key", "value", {"promtoed_tag": "promtoed"})},
    ).process_query(query, HTTPRequestSettings())

    assert query.get_arrayjoin_from_ast() == expected_query.get_arrayjoin_from_ast()
