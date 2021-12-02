from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.processors.mapping_optimizer import MappingOptimizer
from snuba.request.request_settings import HTTPRequestSettings
from snuba.state import set_config
from tests.query.processors.query_builders import build_query

tag_existence_expression = FunctionCall(
    None,
    "notEquals",
    (
        FunctionCall(
            None,
            "ifNull",
            (
                FunctionCall(
                    "_snuba_tags[duration_group]",
                    "arrayElement",
                    (
                        Column(None, None, "tags.value"),
                        FunctionCall(
                            None,
                            "indexOf",
                            (
                                Column(None, None, "tags.key",),
                                Literal(None, "duration_group",),
                            ),
                        ),
                    ),
                ),
                Literal(None, ""),
            ),
        ),
        Literal(None, ""),
    ),
)

tag_equality_expression = FunctionCall(
    None,
    "equals",
    (
        FunctionCall(
            None,
            "ifNull",
            (
                FunctionCall(
                    "_snuba_tags[duration_group]",
                    "arrayElement",
                    (
                        Column(None, None, "tags.value"),
                        FunctionCall(
                            None,
                            "indexOf",
                            (
                                Column(None, None, "tags.key",),
                                Literal(None, "duration_group",),
                            ),
                        ),
                    ),
                ),
                Literal(None, ""),
            ),
        ),
        Literal(None, "<10s"),
    ),
)


query = build_query(
    selected_columns=[Column("count", None, "count")],
    condition=FunctionCall(
        None, "and", (tag_existence_expression, tag_equality_expression,),
    ),
)


def or_exp(op1: Expression, op2: Expression) -> FunctionCall:
    return FunctionCall(None, "or", (op1, op2))


def and_exp(op1: Expression, op2: Expression) -> FunctionCall:
    return FunctionCall(None, "and", (op1, op2))


noop_and = and_exp(Literal(None, True), Literal(None, True))


def test_recursive_useless_condition():
    query = build_query(
        selected_columns=[Column("count", None, "count")],
        condition=and_exp(
            noop_and,
            or_exp(
                and_exp(tag_existence_expression, tag_equality_expression), noop_and
            ),
        ),
    )
    MappingOptimizer(
        column_name="tags",
        hash_map_name="_tags_hash_map",
        killswitch="tags_hash_map_enabled",
    ).process_query(query, HTTPRequestSettings())


def test_bs():
    query = build_query(
        selected_columns=[Column("count", None, "count")],
        condition=FunctionCall(
            None, "and", (tag_existence_expression, tag_equality_expression,),
        ),
    )
    set_config("tags_hash_map_enabled", 1)
    print("BEFORE: \n", query)
    MappingOptimizer(
        column_name="tags",
        hash_map_name="_tags_hash_map",
        killswitch="tags_hash_map_enabled",
    ).process_query(query, HTTPRequestSettings())
    print("<" * 100)
    print("AFTER: \n", query)
    assert "arrayElement" not in repr(query)
