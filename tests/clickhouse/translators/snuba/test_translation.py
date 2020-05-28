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
    col = Column(None, "table", "col")
    translated = SimpleColumnMapper("table", "col", "table2", "col2").attempt_map(
        col, SnubaClickhouseMappingTranslator(TranslationMappers())
    )

    assert translated == Column(None, "table2", "col2")


def test_tag_translation() -> None:
    col = SubscriptableReference(
        "tags[release]", Column(None, None, "tags"), Literal(None, "release")
    )
    translated = TagMapper(None, "tags", None, "tags").attempt_map(
        col, SnubaClickhouseMappingTranslator(TranslationMappers())
    )

    assert translated == FunctionCall(
        "tags[release]",
        "arrayElement",
        (
            Column(None, None, "tags.value"),
            FunctionCall(
                None,
                "indexOf",
                (Column(None, None, "tags.key"), Literal(None, "release")),
            ),
        ),
    )


test_data = [
    (
        "default rule",
        TranslationMappers(columns=[SimpleColumnMapper(None, "col", None, "col2")]),
        Column(None, None, "col3"),
        Column(None, None, "col3"),
    ),
    (
        "simple column",
        TranslationMappers(columns=[SimpleColumnMapper(None, "col", None, "col2")]),
        Column(None, None, "col"),
        Column(None, None, "col2"),
    ),
    (
        "tag mapper",
        TranslationMappers(subscriptables=[TagMapper(None, "tags", "table", "tags")]),
        SubscriptableReference(
            "tags[release]", Column(None, None, "tags"), Literal(None, "release")
        ),
        FunctionCall(
            "tags[release]",
            "arrayElement",
            (
                Column(None, "table", "tags.value"),
                FunctionCall(
                    None,
                    "indexOf",
                    (Column(None, "table", "tags.key"), Literal(None, "release")),
                ),
            ),
        ),
    ),
    (
        "complex translator with tags and columns",
        TranslationMappers(
            subscriptables=[TagMapper(None, "tags", None, "tags")],
            columns=[
                SimpleColumnMapper(None, "col", None, "col2"),
                SimpleColumnMapper(None, "cola", None, "colb"),
            ],
        ),
        FunctionCall(
            None,
            "someFunc",
            (
                FunctionCall(
                    None,
                    "anotherFunc",
                    (Column(None, None, "col"), Literal(None, 123)),
                ),
                CurriedFunctionCall(
                    None,
                    FunctionCall(
                        None,
                        "yetAnotherOne",
                        (
                            SubscriptableReference(
                                "tags[release]",
                                Column(None, None, "tags"),
                                Literal(None, "release"),
                            ),
                        ),
                    ),
                    (Column(None, None, "cola"), Literal(None, 123)),
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
                    (Column(None, None, "col2"), Literal(None, 123)),
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
                                    Column(None, None, "tags.value"),
                                    FunctionCall(
                                        None,
                                        "indexOf",
                                        (
                                            Column(None, None, "tags.key"),
                                            Literal(None, "release"),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                    (Column(None, None, "colb"), Literal(None, 123)),
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
