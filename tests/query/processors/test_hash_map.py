from typing import Optional, Sequence

import pytest

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.translators.snuba.mappers import build_mapping_expr
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as SnubaQuery
from snuba.query.logical import SelectedExpression
from snuba.query.processors.tags_hash_map import TagsHashMapOptimizer
from snuba.request.request_settings import HTTPRequestSettings


def build_query(
    selected_columns: Optional[Sequence[Expression]] = None,
    condition: Optional[Expression] = None,
    having: Optional[Expression] = None,
) -> ClickhouseQuery:
    return ClickhouseQuery(
        SnubaQuery(
            {},
            None,
            selected_columns=[
                SelectedExpression(name=s.alias, expression=s)
                for s in selected_columns or []
            ],
            condition=condition,
            having=having,
        )
    )


def column(name: str, no_alias: bool = False) -> Column:
    return Column(
        alias=name if not no_alias else None, table_name=None, column_name=name
    )


def nested_expression(column: str, key: str) -> FunctionCall:
    return build_mapping_expr(
        alias=f"{column}[{key}]",
        table_name=None,
        col_name=column,
        mapping_key=Literal(None, key),
    )


def nested_condition(
    column_name: str, operator: str, key: str, val: str,
) -> Expression:
    return binary_condition(
        None, operator, nested_expression(column_name, key), Literal(None, val),
    )


TEST_CASES = [
    pytest.param(
        build_query(
            selected_columns=[column("event_id"), nested_expression("tags", "my_tag")],
            condition=binary_condition(
                None, ConditionFunctions.EQ, column("event_id"), Literal(None, "123123")
            ),
        ),
        binary_condition(
            None, ConditionFunctions.EQ, column("event_id"), Literal(None, "123123")
        ),
        id="No tag condition",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=nested_condition(
                "contexts", ConditionFunctions.EQ, "my_ctx", "a"
            ),
        ),
        nested_condition("contexts", ConditionFunctions.EQ, "my_ctx", "a"),
        id="Nested condition on the wrong column",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=nested_condition("tags", ConditionFunctions.EQ, "my_tag", "a"),
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
            condition=binary_condition(
                None,
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
            condition=nested_condition("tags", ConditionFunctions.LIKE, "my_tag", "a"),
        ),
        nested_condition("tags", ConditionFunctions.LIKE, "my_tag", "a"),
        id="Unsupported condition",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                None,
                BooleanFunctions.OR,
                nested_condition("tags", ConditionFunctions.EQ, "my_tag", "a"),
                nested_condition("tags", ConditionFunctions.LIKE, "my_tag2", "b"),
            ),
        ),
        binary_condition(
            None,
            BooleanFunctions.OR,
            nested_condition("tags", ConditionFunctions.EQ, "my_tag", "a"),
            nested_condition("tags", ConditionFunctions.LIKE, "my_tag2", "b"),
        ),
        id="Unsupported and supported conditions",
    ),
    pytest.param(
        build_query(
            selected_columns=[column("event_id")],
            condition=binary_condition(
                None,
                BooleanFunctions.AND,
                nested_condition("tags", ConditionFunctions.EQ, "my_tag", "a"),
                binary_condition(
                    None,
                    ConditionFunctions.LIKE,
                    Column(None, None, "something_else"),
                    Literal(None, "123123"),
                ),
            ),
        ),
        binary_condition(
            None,
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
                None,
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
                None,
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
            None,
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
            condition=nested_condition("tags", ConditionFunctions.EQ, "my_tag", "a"),
            having=binary_condition(
                None,
                ConditionFunctions.EQ,
                FunctionCall(None, "arrayjoin", (Column(None, None, "tags.key"),)),
                Literal(None, "bla"),
            ),
        ),
        nested_condition("tags", ConditionFunctions.EQ, "my_tag", "a"),
        id="Non opimizable having",
    ),
]


@pytest.mark.parametrize("query, expected_condition", TEST_CASES)
def test_tags_hash_map(query: ClickhouseQuery, expected_condition: Expression,) -> None:
    TagsHashMapOptimizer(
        column_name="tags", hash_map_name="_tags_hash_map"
    ).process_query(query, HTTPRequestSettings())

    assert query.get_condition_from_ast() == expected_condition
