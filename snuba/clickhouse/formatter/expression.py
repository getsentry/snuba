import re
from abc import ABC, abstractmethod
from datetime import date, datetime
from functools import lru_cache
from typing import Optional, Sequence, cast

from snuba.clickhouse.escaping import escape_alias, escape_identifier, escape_string
from snuba.query.conditions import (
    BooleanFunctions,
    get_first_level_and_conditions,
    get_first_level_or_conditions,
)
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    DangerousRawSQL,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    JsonPath,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.parsing import ParsingContext

_BETWEEN_SQUARE_BRACKETS_REGEX = re.compile(r"(?<=\[)(.*?)(?=\])")
_SIMPLE_TYPE_RE = re.compile(r"^[a-zA-Z0-9_()\s,]+$")


@lru_cache(maxsize=128)
def _is_simple_type(type_str: str) -> bool:
    return bool(_SIMPLE_TYPE_RE.match(type_str))


class ExpressionFormatterBase(ExpressionVisitor[str], ABC):
    """
    This Visitor implementation is able to format one expression in the Snuba
    Query for Clickhouse.

    The only state maintained is the ParsingContext, which allows us to resolve
    aliases and can be reused when formatting multiple expressions.

    When passing an instance of this class to the accept method of
    the visited expression, the return value is the formatted string.
    """

    def __init__(self, parsing_context: Optional[ParsingContext] = None) -> None:
        self._parsing_context = parsing_context if parsing_context is not None else ParsingContext()

    def _alias(self, formatted_exp: str, alias: Optional[str]) -> str:
        if not alias:
            return formatted_exp
        elif self._parsing_context.is_alias_present(alias):
            ret = escape_alias(alias)
            # This is for the type checker. escape_alias can return None if
            # we pass None. But here we do not pass None so a None return value
            # is not valid.
            assert ret is not None
            return ret
        else:
            self._parsing_context.add_alias(alias)
            return f"({formatted_exp} AS {escape_alias(alias)})"

    @abstractmethod
    def _format_string_literal(self, exp: Literal) -> str:
        raise NotImplementedError

    @abstractmethod
    def _format_number_literal(self, exp: Literal) -> str:
        raise NotImplementedError

    @abstractmethod
    def _format_boolean_literal(self, exp: Literal) -> str:
        raise NotImplementedError

    @abstractmethod
    def _format_datetime_literal(self, exp: Literal) -> str:
        raise NotImplementedError

    @abstractmethod
    def _format_date_literal(self, exp: Literal) -> str:
        raise NotImplementedError

    def visit_literal(self, exp: Literal) -> str:
        if exp.value is None:
            return self._alias("NULL", exp.alias)
        if isinstance(exp.value, bool):
            return self._format_boolean_literal(exp)
        elif isinstance(exp.value, str):
            return self._format_string_literal(exp)
        elif isinstance(exp.value, (int, float)):
            return self._format_number_literal(exp)
        elif isinstance(exp.value, datetime):
            return self._format_datetime_literal(exp)
        elif isinstance(exp.value, date):
            return self._format_date_literal(exp)
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
            # If there is a table name and the column name contains a ".",
            # then we need to escape the column name using alias regex rules
            # to clearly demarcate the table and columns
            ret.append(escape_alias(exp.column_name) or "")
        else:
            # If there is a table name and the column name contains a ".",
            # then we need to escape the column name using alias regex rules
            # otherwise clickhouse will think we are referring to a table
            if "." in exp.column_name:
                ret.append(escape_alias(exp.column_name) or "")
            else:
                ret.append(escape_identifier(exp.column_name) or "")
        ret_unescaped.append(exp.column_name)
        # De-clutter the output query by not applying an alias to a
        # column if the column name is the same as the alias to make
        # the query more readable.
        # This happens often since we apply column aliases during
        # parsing so the names are preserved during query processing.
        if exp.alias != "".join(ret_unescaped):
            return self._alias("".join(ret), exp.alias)
        else:
            return "".join(ret)

    def __visit_params(self, parameters: Sequence[Expression]) -> str:
        ret = [p.accept(self) for p in parameters]
        param_list = ", ".join(ret)
        return f"{param_list}"

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> str:
        # Formatting SubscriptableReference does not make sense for a clickhouse
        # formatter, since the Clickhouse does not support this kind of nodes.
        # The Clickhouse Query AST will not have this node at all so this method will
        # not exist. Still now an implementation that does not throw has to be provided
        # until we actually resolve tags during query translation.
        return f"{self.visit_column(exp.column)}[{self.visit_literal(exp.key)}]"

    def visit_function_call(self, exp: FunctionCall) -> str:
        if exp.function_name == "array":
            # Workaround for https://github.com/ClickHouse/ClickHouse/issues/11622
            # Some distributed queries fail when arrays are passed as array(1,2,3)
            # and work when they are passed as [1, 2, 3]
            return self._alias(f"[{self.__visit_params(exp.parameters)}]", exp.alias)

        if exp.function_name == "tuple" and len(exp.parameters) > 1:
            # Some distributed queries fail when tuples are passed as tuple(1,2,3)
            # and work when they are passed as (1, 2, 3)
            # to be safe, only do this for when a tuple has more than one element otherwise clickhouse
            # will interpret (1) -> 1 which will break things like 1 IN tuple(1)
            return self._alias(f"({self.__visit_params(exp.parameters)})", exp.alias)

        elif exp.function_name == BooleanFunctions.AND:
            formatted = (c.accept(self) for c in get_first_level_and_conditions(exp))
            return " AND ".join(formatted)

        elif exp.function_name == BooleanFunctions.OR:
            formatted = (c.accept(self) for c in get_first_level_or_conditions(exp))
            return f"({' OR '.join(formatted)})"

        ret = f"{escape_identifier(exp.function_name)}({self.__visit_params(exp.parameters)})"
        return self._alias(ret, exp.alias)

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> str:
        int_func = exp.internal_function.accept(self)
        ret = f"{int_func}({self.__visit_params(exp.parameters)})"
        return self._alias(ret, exp.alias)

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
        ret = f"{', '.join(parameters)} -> {exp.transformation.accept(self)}"
        return self._alias(ret, exp.alias)

    def visit_dangerous_raw_sql(self, exp: DangerousRawSQL) -> str:
        """
        Format DangerousRawSQL by passing through the SQL content directly without
        any escaping or validation. This is intentional as DangerousRawSQL is meant
        for pre-validated SQL in query optimization scenarios.
        """
        return self._alias(exp.sql, exp.alias)

    def visit_json_path(self, exp: JsonPath) -> str:
        base_sql = exp.base.accept(self)
        safe_path = exp.path.replace("`", "\\`")
        if exp.return_type is None:
            formatted = f"{base_sql}.`{safe_path}`"
        elif _is_simple_type(exp.return_type):
            formatted = f"{base_sql}.`{safe_path}`::{exp.return_type}"
        else:
            safe_type = exp.return_type.replace("`", "\\`")
            formatted = f"{base_sql}.`{safe_path}`.:`{safe_type}`"
        return self._alias(formatted, exp.alias)


class ClickhouseExpressionFormatter(ExpressionFormatterBase):
    """
    This Formatter produces a properly escaped string. The result should never
    be further escaped. This should be the only place where expression
    escaping happens as it is done by each method that formats a specific
    type of expression.
    """

    def _format_string_literal(self, exp: Literal) -> str:
        return self._alias(escape_string(cast(str, exp.value)), exp.alias)

    def _format_number_literal(self, exp: Literal) -> str:
        return self._alias(str(exp.value), exp.alias)

    def _format_boolean_literal(self, exp: Literal) -> str:
        if exp.value is True:
            return self._alias("true", exp.alias)

        return self._alias("false", exp.alias)

    def _format_datetime_literal(self, exp: Literal) -> str:
        value = cast(datetime, exp.value).replace(tzinfo=None, microsecond=0)
        return self._alias("toDateTime('{}', 'Universal')".format(value.isoformat()), exp.alias)

    def _format_date_literal(self, exp: Literal) -> str:
        return self._alias(
            "toDate('{}', 'Universal')".format(cast(date, exp.value).isoformat()),
            exp.alias,
        )


def _gen_random_number() -> int:
    # https://xkcd.com/221/
    return -1337


class ExpressionFormatterAnonymized(ClickhouseExpressionFormatter):
    def _format_string_literal(self, exp: Literal) -> str:
        return "'$S'"

    def _format_number_literal(self, exp: Literal) -> str:
        return str(_gen_random_number())

    def _anonimize_alias(self, alias: str) -> str:
        # there may be an alias that looks like `snuba_tags[something]`
        # the string between the square brackets has the potential to be PII
        # This function will anonimize that which is between the brackets.
        # this may erroneously anonimize aliases with square brackets
        # if they are input by the user, but that is better than leaking PII
        return _BETWEEN_SQUARE_BRACKETS_REGEX.sub("$A", alias)

    def _alias(self, formatted_exp: str, alias: Optional[str]) -> str:
        if not alias:
            return formatted_exp
        elif self._parsing_context.is_alias_present(alias):
            ret = escape_alias(alias)
            # This is for the type checker. escape_alias can return None if
            # we pass None. But here we do not pass None so a None return value
            # is not valid.
            assert ret is not None
            return ret
        else:
            self._parsing_context.add_alias(alias)
            return f"({formatted_exp} AS {escape_alias(self._anonimize_alias(alias))})"
