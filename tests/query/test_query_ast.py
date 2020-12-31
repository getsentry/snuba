from typing import Any, MutableMapping

import pytest
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.factory import get_dataset
from snuba.pipeline.processors import execute_all_clickhouse_processors
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Table
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.parser import parse_query
from snuba.request.request_settings import HTTPRequestSettings


def test_iterate_over_query() -> None:
    """
    Creates a query with the new AST and iterate over all expressions.
    """
    column1 = Column(None, "t1", "c1")
    column2 = Column(None, "t1", "c2")
    function_1 = FunctionCall("alias", "f1", (column1, column2))
    function_2 = FunctionCall("alias", "f2", (column2,))

    condition = binary_condition(ConditionFunctions.EQ, column1, Literal(None, "1"))

    prewhere = binary_condition(ConditionFunctions.EQ, column2, Literal(None, "2"))

    orderby = OrderBy(OrderByDirection.ASC, function_2)

    query = Query(
        Table("my_table", ColumnSet([])),
        selected_columns=[SelectedExpression("alias", function_1)],
        array_join=None,
        condition=condition,
        groupby=[function_1],
        prewhere=prewhere,
        having=None,
        order_by=[orderby],
    )

    expected_expressions = [
        # selected columns
        column1,
        column2,
        function_1,
        # condition
        column1,
        Literal(None, "1"),
        condition,
        # groupby
        column1,
        column2,
        function_1,
        # order by
        column2,
        function_2,
        # prewhere
        column2,
        Literal(None, "2"),
        prewhere,
    ]

    assert list(query.get_all_expressions()) == expected_expressions


def test_replace_expression() -> None:
    """
    Create a query with the new AST and replaces a function with a different function
    replaces f1(...) with tag(f1)
    """
    column1 = Column(None, "t1", "c1")
    column2 = Column(None, "t1", "c2")
    function_1 = FunctionCall("alias", "f1", (column1, column2))
    function_2 = FunctionCall("alias", "f2", (column2,))

    condition = binary_condition(ConditionFunctions.EQ, function_1, Literal(None, "1"))

    prewhere = binary_condition(ConditionFunctions.EQ, function_1, Literal(None, "2"))

    orderby = OrderBy(OrderByDirection.ASC, function_2)

    query = Query(
        Table("my_table", ColumnSet([])),
        selected_columns=[SelectedExpression("alias", function_1)],
        array_join=None,
        condition=condition,
        groupby=[function_1],
        having=None,
        prewhere=prewhere,
        order_by=[orderby],
    )

    def replace(exp: Expression) -> Expression:
        if isinstance(exp, FunctionCall) and exp.function_name == "f1":
            return FunctionCall(exp.alias, "tag", (Literal(None, "f1"),))
        return exp

    query.transform_expressions(replace)

    expected_query = Query(
        Table("my_table", ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "alias", FunctionCall("alias", "tag", (Literal(None, "f1"),))
            )
        ],
        array_join=None,
        condition=binary_condition(
            ConditionFunctions.EQ,
            FunctionCall("alias", "tag", (Literal(None, "f1"),)),
            Literal(None, "1"),
        ),
        groupby=[FunctionCall("alias", "tag", (Literal(None, "f1"),))],
        prewhere=binary_condition(
            ConditionFunctions.EQ,
            FunctionCall("alias", "tag", (Literal(None, "f1"),)),
            Literal(None, "2"),
        ),
        having=None,
        order_by=[orderby],
    )

    assert (
        query.get_selected_columns_from_ast()
        == expected_query.get_selected_columns_from_ast()
    )
    assert query.get_condition_from_ast() == expected_query.get_condition_from_ast()
    assert query.get_groupby_from_ast() == expected_query.get_groupby_from_ast()
    assert query.get_having_from_ast() == expected_query.get_having_from_ast()
    assert query.get_orderby_from_ast() == expected_query.get_orderby_from_ast()

    assert list(query.get_all_expressions()) == list(
        expected_query.get_all_expressions()
    )


def test_get_all_columns() -> None:
    query_body = {
        "selected_columns": [
            ["f1", ["column1", "column2"], "f1_alias"],
            ["f2", [], "f2_alias"],
            ["formatDateTime", ["timestamp", "'%Y-%m-%d'"], "formatted_time"],
        ],
        "aggregations": [
            ["count", "platform", "platforms"],
            ["uniq", "platform", "uniq_platforms"],
            ["testF", ["platform", ["anotherF", ["field2"]]], "top_platforms"],
        ],
        "conditions": [["tags[sentry:dist]", "IN", ["dist1", "dist2"]]],
        "having": [["times_seen", ">", 1]],
        "groupby": [["format_eventid", ["event_id"]]],
    }
    events = get_dataset("events")
    query = parse_query(query_body, events)

    assert query.get_all_ast_referenced_columns() == {
        Column("_snuba_column1", None, "column1"),
        Column("_snuba_column2", None, "column2"),
        Column("_snuba_platform", None, "platform"),
        Column("_snuba_field2", None, "field2"),
        Column("_snuba_tags", None, "tags"),
        Column("_snuba_times_seen", None, "times_seen"),
        Column("_snuba_event_id", None, "event_id"),
        Column("_snuba_timestamp", None, "timestamp"),
    }

    assert query.get_all_ast_referenced_subscripts() == {
        SubscriptableReference(
            "_snuba_tags[sentry:dist]",
            Column("_snuba_tags", None, "tags"),
            Literal(None, "sentry:dist"),
        )
    }


VALIDATION_TESTS = [
    pytest.param(
        {
            "selected_columns": ["project_id", "event_id"],
            "conditions": [["event_id", "IN", ["a", "b"]]],
        },
        True,
        id="No alias references",
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", ["f", ["event_id"], "not_event"]],
            "conditions": [["not_event", "IN", ["a", "b"]]],
        },
        True,
        id="Alias declared and referenced",
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", ["f", ["event_id"], "event_id"]],
            "conditions": [["event_id", "IN", ["a", "b"]]],
        },
        True,
        id="Alias redefines col and referenced",
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", ["f", ["event_id"], "event_id"]],
            "conditions": [["whatsthis", "IN", ["a", "b"]]],
        },
        False,
        id="Alias referenced and not defined",
    ),
]


@pytest.mark.parametrize("query_body, expected_result", VALIDATION_TESTS)
def test_alias_validation(
    query_body: MutableMapping[str, Any], expected_result: bool
) -> None:
    events = get_dataset("events")
    query = parse_query(query_body, events)
    settings = HTTPRequestSettings()
    query_plan = (
        events.get_default_entity()
        .get_query_pipeline_builder()
        .build_planner(query, settings)
    ).execute()
    execute_all_clickhouse_processors(query_plan, settings)

    assert query_plan.query.validate_aliases() == expected_result
