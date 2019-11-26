from typing import Optional, Sequence
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    Literal,
)
from snuba.query.parsing import ParsingContext
from snuba.clickhouse.escaping import escape_alias, escape_col, escape_string

# Tokens used when formatting. Defining them as constant
# will make it easy (if/when needed) to make some changes
# to the layout of the output.
NULL = "NULL"
COMMA = ","
SPACE = " "
TRUE = "true"
FALSE = "false"
QUOTE = "'"
OPEN_PAREN = "("
CLOSED_PAREN = ")"
DOT = "."


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
            return f"{OPEN_PAREN}{formatted_exp}{SPACE}AS{SPACE}{escape_alias(alias)}{CLOSED_PAREN}"

    def visitLiteral(self, exp: Literal) -> str:
        if exp.value is None:
            return NULL
        elif exp.value is True:
            return TRUE
        elif exp.value is False:
            return FALSE
        elif isinstance(exp.value, str):
            return escape_string(exp.value)
        elif isinstance(exp.value, (int, float)):
            return str(exp.value)
        else:
            raise ValueError(f"Unexpected literal type {type(exp.value)}")

    def visitColumn(self, exp: Column) -> str:
        ret = []
        if exp.table_name:
            ret.append(escape_col(exp.table_name) or "")
            ret.append(DOT)
        ret.append(escape_col(exp.column_name) or "")
        return self.__alias("".join(ret), exp.alias)

    def __visit_params(self, parameters: Sequence[Expression]) -> str:
        ret = [p.accept(self) for p in parameters]
        param_list = ", ".join(ret)
        return f"{OPEN_PAREN}{param_list}{CLOSED_PAREN}"

    def visitFunctionCall(self, exp: FunctionCall) -> str:
        ret = f"{escape_col(exp.function_name)}{self.__visit_params(exp.parameters)}"
        return self.__alias(ret, exp.alias)

    def visitCurriedFunctionCall(self, exp: CurriedFunctionCall) -> str:
        int_func = exp.internal_function.accept(self)
        ret = f"{int_func}{self.__visit_params(exp.parameters)}"
        return self.__alias(ret, exp.alias)
