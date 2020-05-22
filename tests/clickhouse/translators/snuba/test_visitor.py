import pytest

from snuba.clickhouse.translators.snuba.rulesbased import (
    TranslationRules,
    SnubaClickhouseRulesTranslator,
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
    Column("alias", "col", "table"),
    Literal("alias", 123),
    Argument("alias", "arg"),
    SubscriptableReference(
        "tags[asd]", Column(None, "tags", None), Literal(None, "release")
    ),
    FunctionCall(
        "alias",
        "f",
        (
            Column(None, "col", "table"),
            Literal(None, 123),
            FunctionCall(None, "f1", (Column(None, "col2", None),)),
        ),
    ),
    CurriedFunctionCall(
        None,
        FunctionCall(None, "f", (Column(None, "col", None), Literal(None, 12))),
        (Column(None, "col3", None),),
    ),
    Lambda(None, ("a", "b"), FunctionCall(None, "f", (Argument(None, "a"),))),
]


@pytest.mark.parametrize("expression", test_data)
def test_default_translation(expression: Expression) -> None:
    """
    Ensures that a translation that relies on the default translation rules
    produces a deep copy of the original expression.
    """

    translated = expression.accept(SnubaClickhouseRulesTranslator(TranslationRules()))

    assert translated == expression
    for e_translated, e_pre_translation in zip(translated, expression):
        assert e_translated is not e_pre_translation
