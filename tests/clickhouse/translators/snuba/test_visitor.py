import pytest

from snuba.clickhouse.translators.snuba.mapping import (
    SnubaClickhouseMappingTranslator,
    TranslationMappers,
)
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)

test_data = [
    Column("alias", "table", "col"),
    Literal("alias", 123),
    Argument("alias", "arg"),
    SubscriptableReference(
        "tags[asd]", Column(None, None, "tags"), Literal(None, "release")
    ),
    FunctionCall(
        "alias",
        "f",
        (
            Column(None, "table", "col"),
            Literal(None, 123),
            FunctionCall(None, "f1", (Column(None, None, "col2"),)),
        ),
    ),
    CurriedFunctionCall(
        None,
        FunctionCall(None, "f", (Column(None, None, "col"), Literal(None, 12))),
        (Column(None, None, "col3"),),
    ),
    Lambda(None, ("a", "b"), FunctionCall(None, "f", (Argument(None, "a"),))),
]


@pytest.mark.parametrize("expression", test_data)
def test_default_translation(expression: Expression) -> None:
    """
    Ensures that a translation that relies on the default translation rules
    produces a deep copy of the original expression.
    """

    translated = expression.accept(
        SnubaClickhouseMappingTranslator(TranslationMappers())
    )

    assert translated == expression
