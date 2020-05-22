from snuba.clickhouse.query import Expression
from snuba.query.expressions import ExpressionVisitor
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
    CurriedFunctionCall,
    Argument,
    Lambda,
)
from snuba.clickhouse.translators.snuba.rulesbased import (
    TranslationRules,
    SnubaClickhouseRulesTranslator,
)
from snuba.clickhouse.translators.snuba.multistep import (
    MultiStepSnubaClickhouseTranslator,
)
from snuba.clickhouse.translators.snuba.rules import SimpleColumnMapper


class FakeMultiTableStorageTranslator(ExpressionVisitor[Expression]):
    def visitLiteral(self, exp: Literal) -> Expression:
        return exp

    def visitColumn(self, exp: Column) -> Expression:
        if exp.column_name == "col_2":
            return Column(exp.alias, "col_2b", "table_2b")
        else:
            return exp

    def visitSubscriptableReference(self, exp: SubscriptableReference) -> Expression:
        return exp

    def visitFunctionCall(self, exp: FunctionCall) -> Expression:
        return exp

    def visitCurriedFunctionCall(self, exp: CurriedFunctionCall) -> Expression:
        return exp

    def visitArgument(self, exp: Argument) -> Expression:
        return exp

    def visitLambda(self, exp: Lambda) -> Expression:
        return exp


def test_multistep() -> None:
    snuba_clickhouse_rules = TranslationRules(
        columns=[SimpleColumnMapper("col", None, "col_2", None)]
    )
    translator = MultiStepSnubaClickhouseTranslator(
        snuba_steps=[],
        snuba_clickhouse_step=SnubaClickhouseRulesTranslator(snuba_clickhouse_rules),
        clickhouse_steps=[FakeMultiTableStorageTranslator()],
    )
    expression = Column("alias", "col", None)
    new_expression = expression.accept(translator)
    assert new_expression == Column("alias", "col_2b", "table_2b")
