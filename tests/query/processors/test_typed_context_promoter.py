import pytest
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.expressions import Column, Literal
from snuba.query.processors.typed_context_promoter import (
    HexIntContextType,
    PromotionSpec,
    TypedContextPromoter,
)
from snuba.request.request_settings import HTTPRequestSettings
from tests.query.processors.query_builders import (
    build_query,
    column,
    nested_condition,
    nested_expression,
)

TEST_CASES = [
    pytest.param(
        build_query(
            selected_columns=[
                column("event_id"),
                nested_expression("contexts", "trace.trace_id"),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ, column("event_id"), Literal(None, "123123")
            ),
        ),
        build_query(
            selected_columns=[
                column("event_id"),
                nested_expression("contexts", "trace.trace_id"),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ, column("event_id"), Literal(None, "123123")
            ),
        ),
        id="No context in condition",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                column("event_id"),
                nested_expression("contexts", "trace.trace_id"),
            ],
            condition=nested_condition(
                "contexts", "my_ctx", ConditionFunctions.EQ, "a"
            ),
        ),
        build_query(
            selected_columns=[
                column("event_id"),
                nested_expression("contexts", "trace.trace_id"),
            ],
            condition=nested_condition(
                "contexts", "my_ctx", ConditionFunctions.EQ, "a"
            ),
        ),
        id="Wrong context in query",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                column("event_id"),
                nested_expression("contexts", "trace.trace_id"),
            ],
            condition=nested_condition(
                "contexts",
                "trace.trace_id",
                ConditionFunctions.EQ,
                "b0ee5765475f4377be3d8623dd2c034f",
            ),
        ),
        build_query(
            selected_columns=[
                column("event_id"),
                nested_expression("contexts", "trace.trace_id"),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "trace_id"),
                Literal(None, "b0ee5765-475f-4377-be3d-8623dd2c034f"),
            ),
        ),
        id="Context found in condition. Ignored in Select",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                BooleanFunctions.AND,
                nested_condition(
                    "contexts",
                    "trace.trace_id",
                    ConditionFunctions.EQ,
                    "b0ee5765475f4377be3d8623dd2c034f",
                ),
                nested_condition("contexts", "my_ctx", ConditionFunctions.EQ, "a"),
            ),
        ),
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "trace_id"),
                    Literal(None, "b0ee5765-475f-4377-be3d-8623dd2c034f"),
                ),
                nested_condition("contexts", "my_ctx", ConditionFunctions.EQ, "a"),
            ),
        ),
        id="Context found in nested condition. Ignored in Select",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                BooleanFunctions.AND,
                nested_condition(
                    "contexts",
                    "trace.trace_id",
                    ConditionFunctions.EQ,
                    "b0ee5765475f4377be3d8623dd2c034f",
                ),
                nested_condition(
                    "contexts",
                    "trace.span_id",
                    ConditionFunctions.EQ,
                    "a046e6b8f2acdd20",
                ),
            ),
        ),
        build_query(
            selected_columns=[
                column("event_id"),
                nested_expression("contexts", "trace.trace_id"),
            ],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "trace_id"),
                    Literal(None, "b0ee5765-475f-4377-be3d-8623dd2c034f"),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "span_id"),
                    Literal(None, 11549171976458001696),
                ),
            ),
        ),
        id="Both valid contexts found. replace.",
    ),
]


@pytest.mark.parametrize("query, expected_query", TEST_CASES)
def test_tags_hash_map(query: ClickhouseQuery, expected_query: ClickhouseQuery) -> None:
    TypedContextPromoter(
        "contexts", {PromotionSpec("trace.span_id", "span_id", HexIntContextType())},
    ).process_query(query, HTTPRequestSettings())

    assert expected_query.equals(query)
