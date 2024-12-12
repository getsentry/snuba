import pytest

from snuba.clickhouse.columns import ColumnSet, Nested
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.physical.literal_rewriter import LiteralRewriter
from snuba.query.query_settings import HTTPQuerySettings

columns = ColumnSet(
    [
        ("promoted", UInt(8, Modifiers(nullable=True))),
        ("tags", Nested([("key", String()), ("value", String())])),
    ]
)
TABLE = Table("events", columns, storage_key=StorageKey("events"))

test_cases = [
    (
        "not rewritten",
        ClickhouseQuery(
            TABLE,
            selected_columns=[
                SelectedExpression(
                    "tags[foo]",
                    FunctionCall(
                        "tags[foo]",
                        "arrayElement",
                        (
                            Column(None, None, "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(None, None, "tags.key"),
                                    Literal(None, "old_value21"),
                                ),
                            ),
                        ),
                    ),
                )
            ],
        ),
        ClickhouseQuery(
            TABLE,
            selected_columns=[
                SelectedExpression(
                    "tags[foo]",
                    FunctionCall(
                        "tags[foo]",
                        "arrayElement",
                        (
                            Column(None, None, "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(None, None, "tags.key"),
                                    Literal(None, "old_value21"),
                                ),
                            ),
                        ),
                    ),
                )
            ],
        ),
    ),
    (
        "replaced with new literal",
        ClickhouseQuery(
            TABLE,
            selected_columns=[
                SelectedExpression(
                    "tags[old_tag]",
                    FunctionCall(
                        "tags[old_tag]",
                        "arrayElement",
                        (
                            Column(None, "table", "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(None, "table", "tags.key"),
                                    Literal(None, "old_tag"),
                                ),
                            ),
                        ),
                    ),
                )
            ],
        ),
        ClickhouseQuery(
            TABLE,
            selected_columns=[
                SelectedExpression(
                    "tags[old_tag]",
                    FunctionCall(
                        "tags[old_tag]",
                        "arrayElement",
                        (
                            Column(None, "table", "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(None, "table", "tags.key"),
                                    Literal(None, "new_tag"),
                                ),
                            ),
                        ),
                    ),
                )
            ],
        ),
    ),
]


@pytest.mark.parametrize("name, query, expected_query", test_cases)
def test_format_expressions(
    name: str, query: ClickhouseQuery, expected_query: ClickhouseQuery
) -> None:
    LiteralRewriter("project_id", {"old_tag": "new_tag"}, None).process_query(
        query, HTTPQuerySettings()
    )

    assert query.get_selected_columns() == expected_query.get_selected_columns()
