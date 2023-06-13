import pytest

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.processors.physical.mapping_optimizer import MappingOptimizer
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state import set_config
from tests.query.processors.query_builders import (
    build_query,
    column,
    nested_condition,
    nested_expression,
)

TEST_CASES = [
    pytest.param(
        build_query(
            selected_columns=[column("event_id"), nested_expression("tags", "my_tag")],
            condition=binary_condition(
                ConditionFunctions.EQ, column("event_id"), Literal(None, "123123")
            ),
        ),
        binary_condition(
            ConditionFunctions.EQ, column("event_id"), Literal(None, "123123")
        ),
        id="No tag condition",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=nested_condition(
                "contexts", "my_ctx", ConditionFunctions.EQ, "a"
            ),
        ),
        nested_condition("contexts", "my_ctx", ConditionFunctions.EQ, "a"),
        id="Nested condition on the wrong column",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=nested_condition("tags", "my_tag", ConditionFunctions.EQ, "a"),
        ),
        FunctionCall(
            None,
            "has",
            (
                column("_tags_hash_map", True),
                FunctionCall(None, "cityHash64", (Literal(None, "my_tag=a"),)),
            ),
        ),
        id="Optimizable simple condition",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=nested_condition("tags", 1234, ConditionFunctions.EQ, "a"),
        ),
        FunctionCall(
            None,
            "has",
            (
                column("_tags_hash_map", True),
                FunctionCall(None, "cityHash64", (Literal(None, "1234=a"),)),
            ),
        ),
        id="Optimizable simple condition on integer key",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=nested_condition("tags", "my=t\\ag", ConditionFunctions.EQ, "a"),
        ),
        FunctionCall(
            None,
            "has",
            (
                column("_tags_hash_map", True),
                FunctionCall(None, "cityHash64", (Literal(None, "my\=t\\\\ag=a"),)),
            ),
        ),
        id="Optimizable simple escaped condition",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "ifNull",
                    (nested_expression("tags", "my_tag"), Literal(None, "")),
                ),
                Literal(None, "bla"),
            ),
        ),
        FunctionCall(
            None,
            "has",
            (
                column("_tags_hash_map", True),
                FunctionCall(None, "cityHash64", (Literal(None, "my_tag=bla"),)),
            ),
        ),
        id="Condition in a ifNull function",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "ifNull",
                    (nested_expression("tags", 1234), Literal(None, "")),
                ),
                Literal(None, "bla"),
            ),
        ),
        FunctionCall(
            None,
            "has",
            (
                column("_tags_hash_map", True),
                FunctionCall(None, "cityHash64", (Literal(None, "1234=bla"),)),
            ),
        ),
        id="Condition in a ifNull function with integer key",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=nested_condition("tags", "my_tag", ConditionFunctions.LIKE, "a"),
        ),
        nested_condition("tags", "my_tag", ConditionFunctions.LIKE, "a"),
        id="Unsupported condition",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=nested_condition("tags", 1234, ConditionFunctions.LIKE, "a"),
        ),
        nested_condition("tags", 1234, ConditionFunctions.LIKE, "a"),
        id="Unsupported condition with integer key",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                BooleanFunctions.OR,
                nested_condition("tags", "my_tag", ConditionFunctions.EQ, "a"),
                nested_condition("tags", "my_tag2", ConditionFunctions.LIKE, "b"),
            ),
        ),
        binary_condition(
            BooleanFunctions.OR,
            nested_condition("tags", "my_tag", ConditionFunctions.EQ, "a"),
            nested_condition("tags", "my_tag2", ConditionFunctions.LIKE, "b"),
        ),
        id="Unsupported and supported conditions",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                BooleanFunctions.OR,
                nested_condition("tags", 1234, ConditionFunctions.EQ, "a"),
                nested_condition("tags", 4321, ConditionFunctions.LIKE, "b"),
            ),
        ),
        binary_condition(
            BooleanFunctions.OR,
            nested_condition("tags", 1234, ConditionFunctions.EQ, "a"),
            nested_condition("tags", 4321, ConditionFunctions.LIKE, "b"),
        ),
        id="Unsupported and supported conditions with integer",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                BooleanFunctions.AND,
                nested_condition("tags", "my_tag", ConditionFunctions.EQ, "a"),
                binary_condition(
                    ConditionFunctions.LIKE,
                    Column(None, None, "something_else"),
                    Literal(None, "123123"),
                ),
            ),
        ),
        binary_condition(
            BooleanFunctions.AND,
            FunctionCall(
                None,
                "has",
                (
                    column("_tags_hash_map", True),
                    FunctionCall(None, "cityHash64", (Literal(None, "my_tag=a"),)),
                ),
            ),
            binary_condition(
                ConditionFunctions.LIKE,
                Column(None, None, "something_else"),
                Literal(None, "123123"),
            ),
        ),
        id="Supported multiple conditions",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                BooleanFunctions.AND,
                nested_condition("tags", 1234, ConditionFunctions.EQ, "a"),
                binary_condition(
                    ConditionFunctions.LIKE,
                    Column(None, None, "something_else"),
                    Literal(None, "123123"),
                ),
            ),
        ),
        binary_condition(
            BooleanFunctions.AND,
            FunctionCall(
                None,
                "has",
                (
                    column("_tags_hash_map", True),
                    FunctionCall(None, "cityHash64", (Literal(None, "1234=a"),)),
                ),
            ),
            binary_condition(
                ConditionFunctions.LIKE,
                Column(None, None, "something_else"),
                Literal(None, "123123"),
            ),
        ),
        id="Supported multiple conditions with integer keys",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "ifNull",
                    (nested_expression("tags", "my_tag"), Literal(None, "")),
                ),
                Literal(None, ""),
            ),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(
                None,
                "ifNull",
                (nested_expression("tags", "my_tag"), Literal(None, "")),
            ),
            Literal(None, ""),
        ),
        id="Unsupported ifNull condition.",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=nested_condition("tags", "my_tag", ConditionFunctions.EQ, "a"),
            having=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(None, "arrayjoin", (Column(None, None, "tags.key"),)),
                Literal(None, "bla"),
            ),
        ),
        nested_condition("tags", "my_tag", ConditionFunctions.EQ, "a"),
        id="Non optimizable having",
    ),
]


@pytest.mark.parametrize("query, expected_condition", TEST_CASES)
@pytest.mark.redis_db
def test_tags_hash_map(
    query: ClickhouseQuery,
    expected_condition: Expression,
) -> None:
    set_config("tags_hash_map_enabled", 1)
    MappingOptimizer(
        column_name="tags",
        hash_map_name="_tags_hash_map",
        killswitch="tags_hash_map_enabled",
    ).process_query(query, HTTPQuerySettings())

    assert query.get_condition() == expected_condition
