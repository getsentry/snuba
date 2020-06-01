import pytest

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToFunction,
    ColumnToLiteral,
    ColumnToMapping,
    ColumnToColumn,
    SubscriptableMapper,
)
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
    assert ColumnToColumn("table", "col", "table2", "col2").attempt_map(
        Column(None, "table", "col"),
        SnubaClickhouseMappingTranslator(TranslationMappers()),
    ) == Column("table.col", "table2", "col2")


def test_column_literal_translation() -> None:
    assert ColumnToLiteral("table", "col", "my_literal").attempt_map(
        Column("c_alias", "table", "col"),
        SnubaClickhouseMappingTranslator(TranslationMappers()),
    ) == Literal("c_alias", "my_literal")


def test_column_function_translation() -> None:
    assert ColumnToFunction(
        None,
        "ip_address",
        "coalesce",
        (Column(None, None, "ip_address_v4"), Column(None, None, "ip_address_v6")),
    ).attempt_map(
        Column(None, None, "ip_address"),
        SnubaClickhouseMappingTranslator(TranslationMappers()),
    ) == FunctionCall(
        "ip_address",
        "coalesce",
        (Column(None, None, "ip_address_v4"), Column(None, None, "ip_address_v6")),
    )


def test_tag_translation() -> None:
    translated = SubscriptableMapper(None, "tags", None, "tags").attempt_map(
        SubscriptableReference(
            "tags[release]", Column(None, None, "tags"), Literal(None, "release")
        ),
        SnubaClickhouseMappingTranslator(TranslationMappers()),
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


def test_col_tag_translation() -> None:
    translated = ColumnToMapping(
        None, "geo_country_code", None, "contexts", "geo.country_code"
    ).attempt_map(
        Column(None, None, "geo_country_code"),
        SnubaClickhouseMappingTranslator(TranslationMappers()),
    )

    assert translated == FunctionCall(
        "geo_country_code",
        "arrayElement",
        (
            Column(None, None, "contexts.value"),
            FunctionCall(
                None,
                "indexOf",
                (Column(None, None, "contexts.key"), Literal(None, "geo.country_code")),
            ),
        ),
    )


test_data = [
    pytest.param(
        TranslationMappers(columns=[ColumnToColumn(None, "col", None, "col2")]),
        Column(None, None, "col3"),
        Column(None, None, "col3"),
        id="default rule",
    ),
    pytest.param(
        TranslationMappers(columns=[ColumnToColumn(None, "col", None, "col2")]),
        Column(None, None, "col"),
        Column("col", None, "col2"),
        id="simple column",
    ),
    pytest.param(
        TranslationMappers(subscriptables=[SubscriptableMapper(None, "tags", "table", "tags")]),
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
        id="tag mapper",
    ),
    pytest.param(
        TranslationMappers(
            subscriptables=[SubscriptableMapper(None, "tags", None, "tags")],
            columns=[
                ColumnToColumn(None, "col", None, "col2"),
                ColumnToColumn(None, "cola", None, "colb"),
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
                    (Column("col", None, "col2"), Literal(None, 123)),
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
                    (Column("cola", None, "colb"), Literal(None, 123)),
                ),
            ),
        ),
        id="complex translator with tags and columns",
    ),
]


@pytest.mark.parametrize("mappings, expression, expected", test_data)
def test_translation(
    mappings: TranslationMappers,
    expression: Expression,
    expected: ClickhouseExpression,
) -> None:
    translator = SnubaClickhouseMappingTranslator(mappings)
    translated = expression.accept(translator)

    assert translated == expected
