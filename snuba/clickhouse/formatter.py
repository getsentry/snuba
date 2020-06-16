from datetime import date, datetime
from typing import Optional, Sequence

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
from snuba.query.parsing import ParsingContext
from snuba.clickhouse.escaping import escape_alias, escape_identifier, escape_string


class ClickhouseExpressionFormatter(ExpressionVisitor[str]):
    """
    This Visitor implementation is able to format one expression in the Snuba
    Query for Clickhouse.

    The only state maintained is the ParsingContext, which allows us to resolve
    aliases and can be reused when formatting multiple expressions.

    When passing an instance of this class to the accept method of
    the visited expression, the return value is the formatted string.

    This Formatter produces a properly escaped string. The result should never
    be further escaped. This should be the only place where expression
    escaping happens as it is done by each method that formats a specific
    type of expression.
    """

    def __init__(self, parsing_context: Optional[ParsingContext] = None) -> None:
        self.__parsing_context = (
            parsing_context if parsing_context is not None else ParsingContext()
        )

    def __alias(self, formatted_exp: str, alias: Optional[str]) -> str:
        if not alias:
            return formatted_exp
        elif self.__parsing_context.is_alias_present(alias):
            ret = escape_alias(alias)
            # This is for the type checker. escape_alias can return None if
            # we pass None. But here we do not pass None so a None return value
            # is not valid.
            assert ret is not None
            return ret
        else:
            self.__parsing_context.add_alias(alias)
            return f"({formatted_exp} AS {escape_alias(alias)})"

    def visit_literal(self, exp: Literal) -> str:
        if exp.value is None:
            return self.__alias("NULL", exp.alias)
        elif exp.value is True:
            return self.__alias("true", exp.alias)
        elif exp.value is False:
            return self.__alias("false", exp.alias)
        elif isinstance(exp.value, str):
            return self.__alias(escape_string(exp.value), exp.alias)
        elif isinstance(exp.value, (int, float)):
            return self.__alias(str(exp.value), exp.alias)
        elif isinstance(exp.value, datetime):
            value = exp.value.replace(tzinfo=None, microsecond=0)
            return self.__alias(
                "toDateTime('{}', 'Universal')".format(value.isoformat()), exp.alias
            )
        elif isinstance(exp.value, date):
            return self.__alias(
                "toDate('{}', 'Universal')".format(exp.value.isoformat()), exp.alias
            )
        else:
            raise ValueError(f"Unexpected literal type {type(exp.value)}")

    def visit_column(self, exp: Column) -> str:
        ret = []
        ret_unescaped = []
        if exp.table_name:
            ret.append(escape_identifier(exp.table_name) or "")
            ret_unescaped.append(exp.table_name or "")
            ret.append(".")
            ret_unescaped.append(".")
        ret.append(escape_identifier(exp.column_name) or "")
        ret_unescaped.append(exp.column_name)
        # De-clutter the output query by not applying an alias to a
        # column if the column name is the same as the alias to make
        # the query more readable.
        # This happens often since we apply column aliases during
        # parsing so the names are preserved during query processing.
        if exp.alias != "".join(ret_unescaped):
            return self.__alias("".join(ret), exp.alias)
        else:
            return "".join(ret)

    def __visit_params(self, parameters: Sequence[Expression]) -> str:
        ret = [p.accept(self) for p in parameters]
        param_list = ", ".join(ret)
        return f"({param_list})"

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> str:
        # Formatting SubscriptableReference does not make sense for a clickhouse
        # formatter, since the Clickhouse does not support this kind of nodes.
        # The Clickhouse Query AST will not have this node at all so this method will
        # not exist. Still now an implementation that does not throw has to be provided
        # until we actually resolve tags during query translation.
        return f"{self.visit_column(exp.column)}[{self.visit_literal(exp.key)}]"

    def visit_function_call(self, exp: FunctionCall) -> str:
        ret = f"{escape_identifier(exp.function_name)}{self.__visit_params(exp.parameters)}"
        return self.__alias(ret, exp.alias)

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> str:
        int_func = exp.internal_function.accept(self)
        ret = f"{int_func}{self.__visit_params(exp.parameters)}"
        return self.__alias(ret, exp.alias)

    def __escape_identifier_enforce(self, expr: str) -> str:
        ret = escape_identifier(expr)
        # This is for the type checker. escape_identifier can return
        # None if the input is None. Here the input is not None.
        assert ret is not None
        return ret

    def visit_argument(self, exp: Argument) -> str:
        return self.__escape_identifier_enforce(exp.name)

    def visit_lambda(self, exp: Lambda) -> str:
        parameters = [self.__escape_identifier_enforce(v) for v in exp.parameters]
        ret = f"({', '.join(parameters)} -> {exp.transformation.accept(self)})"
        return self.__alias(ret, exp.alias)
