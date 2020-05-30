from typing import Optional, Sequence

from snuba.query.conditions import in_condition
from snuba.query.dsl import arrayElement
from snuba.query.expressions import (
    Argument,
    Column,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)


def array_join(alias: Optional[str], content: Expression) -> Expression:
    return FunctionCall(alias, "arrayJoin", (content,))


def tag_column(alias: Optional[str], content: Expression) -> Expression:
    return _select_column(alias, content, 1)


def value_column(alias: Optional[str], content: Expression) -> Expression:
    return _select_column(alias, content, 2)


def _select_column(
    alias: Optional[str], content: Expression, position: int
) -> Expression:
    return arrayElement(
        alias,
        FunctionCall("all_tags", "arrayJoin", (content,)),
        Literal(None, position),
    )


def map_columns() -> Expression:
    return FunctionCall(
        None,
        "arrayMap",
        (
            Lambda(
                None,
                ("x", "y"),
                FunctionCall(
                    None, "array", (Argument(None, "x"), Argument(None, "y"),),
                ),
            ),
            Column(None, None, "tags.key"),
            Column(None, None, "tags.value"),
        ),
    )


def filter_pairs(tag_keys: Sequence[Literal]) -> Expression:
    return FunctionCall(
        None,
        "arrayFilter",
        (
            Lambda(
                None,
                ("pair",),
                in_condition(
                    None,
                    arrayElement(None, Argument(None, "pair"), Literal(None, 1),),
                    tag_keys,
                ),
            ),
            map_columns(),
        ),
    )


def filter_tag(tag_keys: Sequence[Literal]) -> Expression:
    return FunctionCall(
        None,
        "arrayFilter",
        (
            Lambda(
                None, ("tag",), in_condition(None, Argument(None, "tag"), tag_keys),
            ),
            Column(None, None, "tags.key"),
        ),
    )
