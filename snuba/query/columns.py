import re

from typing import OrderedDict
import _strptime  # NOQA fixes _strptime deferred import issue

from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.query.schema import POSITIVE_OPERATORS
from snuba.util import (
    alias_expr,
    escape_alias,
    escape_literal,
    function_expr,
    is_condition,
    is_function,
    NEGATE_RE,
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
        expr = dataset.column_expr(column_name, query, parsing_context)
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
    function_tuple = is_function(expr, depth)
    if function_tuple is None:
        raise ValueError(
            "complex_column_expr was given an expr %s that is not a function at depth %d."
            % (expr, depth)
        )

    name, args, alias = function_tuple
    out = []
    i = 0
    while i < len(args):
        next_2 = args[i : i + 2]
        if is_function(next_2, depth + 1):
            out.append(
                complex_column_expr(dataset, next_2, query, parsing_context, depth + 1)
            )
            i += 2
        else:
            nxt = args[i]
            if is_function(nxt, depth + 1):  # Embedded function
                out.append(
                    complex_column_expr(dataset, nxt, query, parsing_context, depth + 1)
                )
            elif isinstance(nxt, str):
                out.append(column_expr(dataset, nxt, query, parsing_context))
            else:
                out.append(escape_literal(nxt))
            i += 1

    ret = function_expr(name, ", ".join(out))
    if alias:
        ret = alias_expr(ret, alias, parsing_context)
    return ret


def conditions_expr(
    dataset, conditions, query: Query, parsing_context: ParsingContext, depth=0
):
    """
    Return a boolean expression suitable for putting in the WHERE clause of the
    query.  The expression is constructed by ANDing groups of OR expressions.
    Expansion of columns is handled, as is replacement of columns with aliases,
    if the column has already been expanded and aliased elsewhere.
    """
    from snuba.clickhouse.columns import Array

    if not conditions:
        return ""

    if depth == 0:
        # dedupe conditions at top level, but keep them in order
        sub = OrderedDict(
            (conditions_expr(dataset, cond, query, parsing_context, depth + 1), None)
            for cond in conditions
        )
        return " AND ".join(s for s in sub.keys() if s)
    elif is_condition(conditions):
        lhs, op, lit = dataset.process_condition(conditions)

        # facilitate deduping IN conditions by sorting them.
        if op in ("IN", "NOT IN") and isinstance(lit, tuple):
            lit = tuple(sorted(lit))

        # If the LHS is a simple column name that refers to an array column
        # (and we are not arrayJoining on that column, which would make it
        # scalar again) and the RHS is a scalar value, we assume that the user
        # actually means to check if any (or all) items in the array match the
        # predicate, so we return an `any(x == value for x in array_column)`
        # type expression. We assume that operators looking for a specific value
        # (IN, =, LIKE) are looking for rows where any array value matches, and
        # exclusionary operators (NOT IN, NOT LIKE, !=) are looking for rows
        # where all elements match (eg. all NOT LIKE 'foo').
        columns = dataset.get_dataset_schemas().get_read_schema().get_columns()
        if (
            isinstance(lhs, str)
            and lhs in columns
            and isinstance(columns[lhs].type, Array)
            and columns[lhs].base_name != query.get_arrayjoin()
            and not isinstance(lit, (list, tuple))
        ):
            any_or_all = "arrayExists" if op in POSITIVE_OPERATORS else "arrayAll"
            return "{}(x -> assumeNotNull(x {} {}), {})".format(
                any_or_all,
                op,
                escape_literal(lit),
                column_expr(dataset, lhs, query, parsing_context),
            )
        else:
            return "{} {} {}".format(
                column_expr(dataset, lhs, query, parsing_context),
                op,
                escape_literal(lit),
            )

    elif depth == 1:
        sub = (
            conditions_expr(dataset, cond, query, parsing_context, depth + 1)
            for cond in conditions
        )
        sub = [s for s in sub if s]
        res = " OR ".join(sub)
        return "({})".format(res) if len(sub) > 1 else res
    else:
        raise InvalidConditionException(str(conditions))
