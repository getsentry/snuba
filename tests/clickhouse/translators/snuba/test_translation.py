import pytest

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.clickhouse.translators.snuba.mappers import SimpleColumnMapper, TagMapper
from snuba.clickhouse.translators.snuba.mapping import (
    SnubaClickhouseMappingTranslator,
    TranslationMappers,
)
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)


def test_column_translation() -> None:
    col = Column(None, "col", "table")
    translated = SimpleColumnMapper("col", "table", "col2", "table2").attempt_map(
        col, SnubaClickhouseMappingTranslator(TranslationMappers())
    )

    assert translated == Column(None, "col2", "table2")


def test_tag_translation() -> None:
    col = SubscriptableReference(
        "tags[release]", Column(None, "tags", None), Literal(None, "release")
    )
    translated = TagMapper("tags", None, "tags", None).attempt_map(
        col, SnubaClickhouseMappingTranslator(TranslationMappers())
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
        "default rule",
        TranslationMappers(columns=[SimpleColumnMapper("col", None, "col2", None)]),
        Column(None, "col3", None),
        Column(None, "col3", None),
    ),
    (
        "simple column",
        TranslationMappers(columns=[SimpleColumnMapper("col", None, "col2", None)]),
        Column(None, "col", None),
        Column(None, "col2", None),
    ),
    (
        "tag mapper",
        TranslationMappers(subscriptables=[TagMapper("tags", None, "tags", "table")]),
        SubscriptableReference(
            "tags[release]", Column(None, "tags", None), Literal(None, "release")
        ),
        FunctionCall(
            "tags[release]",
            "arrayElement",
            (
                Column(None, "tags.value", "table"),
                FunctionCall(
                    None,
                    "indexOf",
                    (Column(None, "tags.key", "table"), Literal(None, "release")),
                ),
            ),
        ),
    ),
    (
        "complex translator with tags and columns",
        TranslationMappers(
            subscriptables=[TagMapper("tags", None, "tags", None)],
            columns=[
                SimpleColumnMapper("col", None, "col2", None),
                SimpleColumnMapper("cola", None, "colb", None),
            ],
        ),
        FunctionCall(
            None,
            "someFunc",
            (
                FunctionCall(
                    None,
                    "anotherFunc",
                    (Column(None, "col", None), Literal(None, 123)),
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
                    (Column(None, "cola", None), Literal(None, 123)),
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
                    (Column(None, "col2", None), Literal(None, 123)),
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
                    (Column(None, "colb", None), Literal(None, 123)),
                ),
            ),
        ),
    ),
]


@pytest.mark.parametrize("name, mappings, expression, expected", test_data)
def test_translation(
    name: str,
    mappings: TranslationMappers,
    expression: Expression,
    expected: ClickhouseExpression,
) -> None:
    translator = SnubaClickhouseMappingTranslator(mappings)
    translated = expression.accept(translator)

    assert translated == expected
