import pytest

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.query.expressions import (
    Column,
    Literal,
    Argument,
    SubscriptableReference,
    FunctionCall,
    CurriedFunctionCall,
    Lambda,
    Expression,
)
from snuba.datasets.plans.translator.visitor import ExpressionMappingSpec
from snuba.clickhouse.translator.visitor import ClickhouseExpressionVisitor
from snuba.clickhouse.translator.rules import ColumnMapper, TagMapper


def test_column_translation() -> None:
    col = Column(None, "col", "table")
    translated = ColumnMapper("col", "table", "col2", "table2").attemptMap(col)

    assert translated == Column(None, "col2", "table2")


def test_tag_translation() -> None:
    col = SubscriptableReference(
        "tags[release]", Column(None, "tags", None), Literal(None, "release")
    )
    mappings = ExpressionMappingSpec(
        columns=[],
        literals=[],
        functions=[],
        subscriptables=[],
        curried_functions=[],
        arguments=[],
        lambdas=[],
    )
    translated = TagMapper("tags", "tags").attemptMap(
        col, ClickhouseExpressionVisitor(mappings)
    )

    assert translated == FunctionCall(
        "tags[release]",
        "arrayElement",
        (
            Column(None, "tags.value", None),
            FunctionCall(
                None,
                "indexOf",
                (Column(None, "tags.key", None), Literal(None, "release")),
            ),
        ),
    )


test_data = [
    (
        ExpressionMappingSpec(
            columns=[ColumnMapper("col", None, "col2", None)],
            literals=[],
            functions=[],
            subscriptables=[],
            curried_functions=[],
            arguments=[],
            lambdas=[],
        ),
        Column(None, "col3", None),
        Column(None, "col3", None),
    ),
    (
        ExpressionMappingSpec(
            columns=[ColumnMapper("col", None, "col2", None)],
            literals=[],
            functions=[],
            subscriptables=[],
            curried_functions=[],
            arguments=[],
            lambdas=[],
        ),
        Column(None, "col", None),
        Column(None, "col2", None),
    ),
    (
        ExpressionMappingSpec(
            columns=[],
            literals=[],
            functions=[],
            subscriptables=[TagMapper("tags", "tags")],
            curried_functions=[],
            arguments=[],
            lambdas=[],
        ),
        SubscriptableReference(
            "tags[release]", Column(None, "tags", None), Literal(None, "release")
        ),
        FunctionCall(
            "tags[release]",
            "arrayElement",
            (
                Column(None, "tags.value", None),
                FunctionCall(
                    None,
                    "indexOf",
                    (Column(None, "tags.key", None), Literal(None, "release")),
                ),
            ),
        ),
    ),
    (
        ExpressionMappingSpec(
            columns=[
                ColumnMapper("col", None, "col2", None),
                ColumnMapper("cola", None, "colb", None),
            ],
            literals=[],
            functions=[],
            subscriptables=[TagMapper("tags", "tags")],
            curried_functions=[],
            arguments=[],
            lambdas=[],
        ),
        FunctionCall(
            None,
            "someFunc",
            (
                FunctionCall(
                    None,
                    "anotherFunc",
                    (Column(None, "col", None), Literal(None, 123),),
                ),
                CurriedFunctionCall(
                    None,
                    FunctionCall(
                        None,
                        "yetAnotherOne",
                        (
                            SubscriptableReference(
                                "tags[release]",
                                Column(None, "tags", None),
                                Literal(None, "release"),
                            ),
                        ),
                    ),
                    (Column(None, "cola", None), Literal(None, 123),),
                ),
            ),
        ),
        FunctionCall(
            None,
            "someFunc",
            (
                FunctionCall(
                    None,
                    "anotherFunc",
                    (Column(None, "col2", None), Literal(None, 123),),
                ),
                CurriedFunctionCall(
                    None,
                    FunctionCall(
                        None,
                        "yetAnotherOne",
                        (
                            FunctionCall(
                                "tags[release]",
                                "arrayElement",
                                (
                                    Column(None, "tags.value", None),
                                    FunctionCall(
                                        None,
                                        "indexOf",
                                        (
                                            Column(None, "tags.key", None),
                                            Literal(None, "release"),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                    (Column(None, "colb", None), Literal(None, 123),),
                ),
            ),
        ),
    ),
]


@pytest.mark.parametrize("mappings, expression, expected", test_data)
def test_translation(
    mappings: ExpressionMappingSpec[ClickhouseExpression],
    expression: Expression,
    expected: ClickhouseExpression,
) -> None:
    translator = ClickhouseExpressionVisitor(mappings)
    translated = expression.accept(translator)

    assert translated == expected
