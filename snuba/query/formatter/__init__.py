from typing import Mapping, Any, Union, Sequence, Optional

from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)

TExpression = Union[str, Mapping[str, Any], Sequence[Any]]


class PrettyPrintExpressionFormatter(ExpressionVisitor[TExpression]):
    def __aliasify_str(self, formatted: str, alias: Optional[str]) -> str:
        return formatted if alias is None else f"({formatted} AS {alias})"

    def visit_literal(self, exp: Literal) -> str:
        return self.__aliasify_str(str(exp), exp.alias)

    def visit_column(self, exp: Column) -> str:
        return self.__aliasify_str(
            f"{exp.table_name + '.' if exp.table_name else ''}{exp.column_name}",
            exp.alias,
        )

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> str:
        return self.__aliasify_str(
            f"{self.visit_column(exp.column)}[{self.visit_literal(exp.key)}]", exp.alias
        )

    def visit_function_call(self, exp: FunctionCall) -> str:
        pass

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> str:
        pass

    def visit_argument(self, exp: Argument) -> str:
        pass

    def visit_lambda(self, exp: Lambda) -> str:
        pass
