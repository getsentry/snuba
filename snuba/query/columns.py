import numbers
import re

from datetime import date, datetime
from typing import Any, List, Optional, Tuple, Union
import _strptime  # NOQA fixes _strptime deferred import issue

from snuba import state
from snuba.clickhouse.escaping import escape_alias, NEGATE_RE
from snuba.query.conditions import FUNCTION_TO_OPERATOR
from snuba.query.logical import Query
from snuba.query.parser.functions import parse_function
from snuba.query.parsing import ParsingContext
from snuba.query.schema import POSITIVE_OPERATORS
from snuba.query.parser.functions import function_expr
from snuba.util import (
    alias_expr,
    escape_literal,
    is_function,
    QUOTED_LITERAL_RE,
)

QUALIFIED_COLUMN_REGEX = re.compile(
    r"^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z0-9_\.\[\]]+)$"
)


class InvalidConditionException(Exception):
    pass


def column_expr(
    dataset,
    column_name,
    query: Query,
    parsing_context: ParsingContext,
    alias=None,
    aggregate=None,
):
    """
    Certain special column names expand into more complex expressions. Return
    a 2-tuple of:
        (expanded column expression, sanitized alias)

    Needs the body of the request for some extra data used to expand column expressions.
    """
    assert column_name or aggregate
    assert not aggregate or (aggregate and (column_name or alias))
    column_name = column_name or ""

    if is_function(column_name, 0):
        return complex_column_expr(dataset, column_name, query, parsing_context)
    elif isinstance(column_name, (list, tuple)) and aggregate:
        return complex_column_expr(
            dataset, [aggregate, column_name, alias], query, parsing_context
        )
    elif isinstance(column_name, str) and QUOTED_LITERAL_RE.match(column_name):
        return escape_literal(column_name[1:-1])
    else:
        if state.get_config("fix_orderby_col_processing", 1):
            # column_name may be prefixed by `-` if this is in an ORDER BY clause that
            # is not present elsewhere in the query (thus was not given an alias).
            # This means we need to strip it from the column_name we pass to the column_expr
            # method and add it back to the result since the column_expr functions should
            # not deal with the ORDER BY syntax.
            match = NEGATE_RE.match(column_name)
            assert (
                match
            ), f"Invalid column format: {column_name}. Cannot strip the order by prefix"
            negate, col = match.groups()
        else:
            negate = ""
            col = column_name
        expr = f"{negate}{dataset.column_expr(col, query, parsing_context)}"

    if aggregate:
        expr = function_expr(aggregate, expr)

    # in the ORDER BY clause, column_expr may receive column names prefixed with
    # `-`. This is meant to be used for ORDER BY ... DESC.
    # This means we need to keep the `-` outside of the aliased expression when
    # we produce something like (COL AS alias) otherwise we build an invalid
    # syntax.
    # Worse, since escape_alias already does half of this work and keeps `-`
    # outside of the escaped expression we end up in this situation:
    #
    # -events.event_id becomes (-events.event_id AS -`events.event_id`)
    #
    # Thus here we strip the `-` before processing escaping and aliases and we
    # attach it back to the expression right before returning so that
    # -events.event_id becomes -(events.event_id AS `events.event_id`)
    # or
    # -`events.event_id`
    # if the alias already existed.
    #
    # The proper solution would be to strip the `-` before getting to column
    # processing, but this will be done with the new column abstraction.
    negate, col = NEGATE_RE.match(column_name).groups()
    alias = escape_alias(alias or col)
    expr_negate, expr = NEGATE_RE.match(expr).groups()
    # expr_negate and negate should never be inconsistent with each other. Though
    # will ensure this works properly before moving the `-` stripping at the beginning
    # of the method to cover tags as well.
    return f"{negate or expr_negate}{alias_expr(expr, alias, parsing_context)}"


def complex_column_expr(
    dataset, expr, query: Query, parsing_context: ParsingContext, depth=0
):
    def column_builder(val: str) -> Any:
        return column_expr(dataset, val, query, parsing_context)

    def literal_builder(
        val: Optional[Union[str, datetime, date, List[Any], Tuple[Any], numbers.Number]]
    ) -> Any:
        return escape_literal(val)

    def output_builder(alias: Optional[str], name: str, params: List[Any]) -> Any:
        ret = function_expr(name, ", ".join(params))
        if alias:
            ret = alias_expr(ret, alias, parsing_context)
        return ret

    def unpack_array_condition_builder(
        lhs: str, func: str, literal: Any, alias: Optional[str]
    ) -> Any:
        op = FUNCTION_TO_OPERATOR[func]
        any_or_all = "arrayExists" if op in POSITIVE_OPERATORS else "arrayAll"
        ret = (
            f"{any_or_all}(x -> assumeNotNull(x {op} {escape_literal(literal)}), {lhs})"
        )
        if alias:
            ret = alias_expr(ret, alias, parsing_context)
        return ret

    return parse_function(
        output_builder,
        column_builder,
        literal_builder,
        unpack_array_condition_builder,
        dataset.get_abstract_columnset(),
        query.get_arrayjoin(),
        expr,
        depth,
    )
