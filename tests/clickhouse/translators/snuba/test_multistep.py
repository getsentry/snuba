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
from snuba.clickhouse.translators.snuba.rulesbased import TranslationRules
from snuba.clickhouse.translators.snuba.rules import SimpleColumnMapper


class FakeMultiTableStorageTranslator(ExpressionVisitor[Expression]):
    def visitLiteral(self, exp: Literal) -> Expression:
        return exp.accept(self)

    def visitColumn(self, exp: Column) -> Expression:
        return exp.accept(self)

    def visitSubscriptableReference(self, exp: SubscriptableReference) -> Expression:
        return exp.accept(self)

    def visitFunctionCall(self, exp: FunctionCall) -> Expression:
        return Column(exp.alias, "some_preaggregated_col", "a_table")

    def visitCurriedFunctionCall(self, exp: CurriedFunctionCall) -> Expression:
        return exp.accept(self)

    def visitArgument(self, exp: Argument) -> Expression:
        return exp.accept(self)

    def visitLambda(self, exp: Lambda) -> Expression:
        return exp.accept(self)
