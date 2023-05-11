import pytest

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToCurriedFunction,
    ColumnToFunction,
    ColumnToFunctionOnColumn,
    ColumnToLiteral,
    ColumnToMapping,
    FunctionNameMapper,
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
        Column("table.col", "table", "col"),
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
        Column("ip_address", None, "ip_address"),
        SnubaClickhouseMappingTranslator(TranslationMappers()),
    ) == FunctionCall(
        "ip_address",
        "coalesce",
        (Column(None, None, "ip_address_v4"), Column(None, None, "ip_address_v6")),
    )


def test_column_curried_function_translation() -> None:
    assert ColumnToCurriedFunction(
        None,
        "duration_quantiles",
        FunctionCall(
            None,
            "quantilesIfMerge",
            (
                Literal(None, 0.5),
                Literal(None, 0.9),
            ),
        ),
        (Column(None, None, "duration_quantiles"),),
    ).attempt_map(
        Column("duration_quantiles", None, "duration_quantiles"),
        SnubaClickhouseMappingTranslator(TranslationMappers()),
    ) == CurriedFunctionCall(
        "duration_quantiles",
        FunctionCall(
            None,
            "quantilesIfMerge",
            (
                Literal(None, 0.5),
                Literal(None, 0.9),
            ),
        ),
        (Column(None, None, "duration_quantiles"),),
    )


def test_column_function_column() -> None:
    assert ColumnToFunctionOnColumn(
        None,
        "tags_key",
        "arrayJoin",
        "tags.key",
    ).attempt_map(
        Column("tags_key", None, "tags_key"),
        SnubaClickhouseMappingTranslator(TranslationMappers()),
    ) == FunctionCall(
        "tags_key",
        "arrayJoin",
        (Column(None, None, "tags.key"),),
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


def _get_nullable_expr(alias: str) -> FunctionCall:
    return FunctionCall(
        alias,
        "if",
        (
            FunctionCall(
                None,
                "has",
                (Column(None, None, "measurements.key"), Literal(None, "lcp")),
            ),
            FunctionCall(
                None,
                "arrayElement",
                (
                    Column(None, None, "measurements.value"),
                    FunctionCall(
                        None,
                        "indexOf",
                        (Column(None, None, "measurements.key"), Literal(None, "lcp")),
                    ),
                ),
            ),
            Literal(None, None),
        ),
    )


def test_nullable_nested_translation() -> None:
    translated = SubscriptableMapper(
        None, "measurements", None, "measurements", nullable=True
    ).attempt_map(
        SubscriptableReference(
            "measurements[lcp]",
            Column(None, None, "measurements"),
            Literal(None, "lcp"),
        ),
        SnubaClickhouseMappingTranslator(TranslationMappers()),
    )

    assert translated == _get_nullable_expr("measurements[lcp]")


def test_col_tag_translation() -> None:
    translated = ColumnToMapping(
        None, "geo_country_code", None, "contexts", "geo.country_code"
    ).attempt_map(
        Column("geo_country_code", None, "geo_country_code"),
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


def test_col_nullable_measurements_translation() -> None:
    translated = ColumnToMapping(
        None, "lcp", None, "measurements", "lcp", nullable=True
    ).attempt_map(
        Column("lcp", None, "lcp"),
        SnubaClickhouseMappingTranslator(TranslationMappers()),
    )

    assert translated == _get_nullable_expr("lcp")


test_data = [
    pytest.param(
        TranslationMappers(columns=[ColumnToColumn(None, "col", None, "col2")]),
        Column(None, None, "col3"),
        Column(None, None, "col3"),
        id="default rule",
    ),
    pytest.param(
        TranslationMappers(columns=[ColumnToColumn(None, "col", None, "col2")]),
        Column("col", None, "col"),
        Column("col", None, "col2"),
        id="simple column",
    ),
    pytest.param(
        TranslationMappers(
            subscriptables=[SubscriptableMapper(None, "tags", "table", "tags")]
        ),
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
            subscriptables=[
                SubscriptableMapper(None, "tags", "table", "tags", "special_value")
            ]
        ),
        SubscriptableReference(
            "tags[release]", Column(None, None, "tags"), Literal(None, "release")
        ),
        FunctionCall(
            "tags[release]",
            "arrayElement",
            (
                Column(None, "table", "tags.special_value"),
                FunctionCall(
                    None,
                    "indexOf",
                    (Column(None, "table", "tags.key"), Literal(None, "release")),
                ),
            ),
        ),
        id="tag mapper, custom value subcolumn",
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
                    (Column("col", None, "col"), Literal(None, 123)),
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
                    (Column("cola", None, "cola"), Literal(None, 123)),
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
    pytest.param(
        TranslationMappers(
            functions=[FunctionNameMapper("uniq", "uniqCombined64Merge")],
            columns=[ColumnToColumn(None, "col", None, "col2")],
        ),
        FunctionCall("test", "uniq", (Column(None, None, "col"),)),
        FunctionCall("test", "uniqCombined64Merge", (Column(None, None, "col2"),)),
        id="Function name mapper",
    ),
    pytest.param(
        TranslationMappers(
            functions=[FunctionNameMapper("quantiles", "quantilesMerge")],
        ),
        CurriedFunctionCall(
            "test",
            FunctionCall(None, "quantiles", (Literal(None, 0.99),)),
            (Column(None, None, "value"),),
        ),
        CurriedFunctionCall(
            "test",
            FunctionCall(None, "quantilesMerge", (Literal(None, 0.99),)),
            (Column(None, None, "value"),),
        ),
        id="Curried function relies on Function name mapper",
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
