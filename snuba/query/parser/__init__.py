import logging
import re
from dataclasses import replace
from typing import Any, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from snuba import environment
from snuba.clickhouse.escaping import NEGATE_RE
from snuba.datasets.dataset import Dataset
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
from snuba.query.logical import OrderBy, OrderByDirection, Query, SelectedExpression
from snuba.query.parser.conditions import parse_conditions_to_expr
from snuba.query.parser.exceptions import (
    AliasShadowingException,
    CyclicAliasException,
    ParsingException,
)
from snuba.query.parser.expressions import parse_aggregation, parse_expression
from snuba.query.parser.validation import validate_query
from snuba.util import is_function, to_list, tuplify
from snuba.utils.metrics.backends.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)

metrics = MetricsWrapper(environment.metrics, "parser")


def parse_query(body: MutableMapping[str, Any], dataset: Dataset) -> Query:
    """
    Parses the query body generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.

    Parsing includes two phases. The first transforms the json body into
    a minimal query Object resolving expressions, conditions, etc.
    The second phase performs some query processing to provide a sane
    query to the dataset specific section.
    - It prevents alias shadowing.
    - It transforms columns from the tags[asd] form into
      SubscriptableReference.
    - Applies aliases to all columns that do not have one and that do not
      represent a reference to an existing alias.
      During query processing a column can be transformed into a different
      expression. It is essential to preserve the original column name so
      that the result set still has a column with the name provided by the
      user no matter on which transformation we applied.
      By applying aliases at this stage every processor just needs to
      preserve them to guarantee the correctness of the query.
    - Expands all the references to aliases by inlining the expression
      to make aliasing transparent to all query processing phases.
      References to aliases are reintroduced at the end of the query
      processing.
      Alias references are packaged back at the end of processing.
    """
    query = _parse_query_impl(body, dataset)
    # These are the post processing phases
    _validate_empty_table_names(query)
    _validate_aliases(query)
    _parse_subscriptables(query)
    _apply_column_aliases(query)
    _expand_aliases(query)
    # WARNING: These steps above assume table resolution did not happen
    # yet. If it is put earlier than here (unlikely), we need to adapt them.
    _deescape_aliases(query)
    validate_query(query, dataset)
    return query


def _parse_query_impl(body: MutableMapping[str, Any], dataset: Dataset) -> Query:
    def build_selected_expressions(
        raw_expressions: Sequence[Any],
    ) -> List[SelectedExpression]:
        output = []
        for raw_expression in raw_expressions:
            exp = parse_expression(
                tuplify(raw_expression), dataset.get_abstract_columnset()
            )
            output.append(
                SelectedExpression(
                    # An expression in the query can be a string or a
                    # complex list with an alias. In the second case
                    # we trust the parser to find the alias.
                    name=raw_expression
                    if isinstance(raw_expression, str)
                    else exp.alias,
                    expression=exp,
                )
            )
        return output

    aggregations = []
    for aggregation in body.get("aggregations", []):
        if not isinstance(aggregation, Sequence):
            raise ParsingException(
                (
                    f"Invalid aggregation structure {aggregation}. "
                    "It must be a sequence containing expression, column and alias."
                )
            )
        aggregation_function = aggregation[0]
        column_expr = aggregation[1]
        column_expr = column_expr if column_expr else []
        alias = aggregation[2]
        alias = alias if alias else None

        aggregations.append(
            SelectedExpression(
                name=alias,
                expression=parse_aggregation(
                    aggregation_function,
                    column_expr,
                    alias,
                    dataset.get_abstract_columnset(),
                ),
            )
        )

    groupby_clause = build_selected_expressions(to_list(body.get("groupby", [])))

    select_clause = (
        groupby_clause
        + aggregations
        + build_selected_expressions(body.get("selected_columns", []))
    )

    arrayjoin = body.get("arrayjoin")
    if arrayjoin:
        array_join_expr: Optional[Expression] = parse_expression(
            body["arrayjoin"], dataset.get_abstract_columnset()
        )
    else:
        array_join_expr = None
        for select_expr in select_clause:
            if isinstance(select_expr.expression, FunctionCall):
                if select_expr.expression.function_name == "arrayJoin":
                    if arrayjoin:
                        raise ParsingException(
                            "Only one arrayJoin(...) call is allowed in a query."
                        )

                    parameters = select_expr.expression.parameters
                    if len(parameters) != 1 or not isinstance(parameters[0], Column):
                        raise ParsingException(
                            "arrayJoin(...) only accepts a single column as a parameter."
                        )
                    arrayjoin = select_expr.expression.parameters[0].column_name

    where_expr = parse_conditions_to_expr(
        body.get("conditions", []), dataset, arrayjoin
    )
    having_expr = parse_conditions_to_expr(body.get("having", []), dataset, arrayjoin)

    orderby_exprs = []
    for orderby in to_list(body.get("orderby", [])):
        if isinstance(orderby, str):
            match = NEGATE_RE.match(orderby)
            if match is None:
                raise ParsingException(
                    (
                        f"Invalid Order By clause {orderby}. If the Order By is a string, "
                        "it must respect the format `[-]column`"
                    )
                )
            direction, col = match.groups()
            orderby = col
        elif is_function(orderby):
            match = NEGATE_RE.match(orderby[0])
            if match is None:
                raise ParsingException(
                    (
                        f"Invalid Order By clause {orderby}. If the Order By is an expression, "
                        "the function name must respect the format `[-]func_name`"
                    )
                )
            direction, col = match.groups()
            orderby = [col] + orderby[1:]
        else:
            raise ParsingException(
                (
                    f"Invalid Order By clause {orderby}. The Clause was neither "
                    "a string nor a function call."
                )
            )
        orderby_parsed = parse_expression(
            tuplify(orderby), dataset.get_abstract_columnset()
        )
        orderby_exprs.append(
            OrderBy(
                OrderByDirection.DESC if direction == "-" else OrderByDirection.ASC,
                orderby_parsed,
            )
        )

    return Query(
        body,
        None,
        selected_columns=select_clause,
        array_join=array_join_expr,
        condition=where_expr,
        groupby=[g.expression for g in groupby_clause],
        having=having_expr,
        order_by=orderby_exprs,
    )


def _validate_empty_table_names(query: Query) -> None:
    found_table_names = set()
    for e in query.get_all_expressions():
        if isinstance(e, Column) and e.table_name:
            found_table_names.add(e.table_name)

    if found_table_names:
        logger.warning(
            "Table names already populated before alias resolution",
            extra={"names": found_table_names},
        )


def _validate_aliases(query: Query) -> None:
    """
    Ensures that no alias has been defined multiple times for different
    expressions in the query. Thus rejecting queries with shadowing.
    """
    all_declared_aliases: MutableMapping[str, Expression] = {}
    for exp in query.get_all_expressions():
        if exp.alias is not None:
            if exp.alias == "":
                # TODO: Enforce this in the parser when we are sure it is not
                # happening.
                metrics.increment("empty_alias")

            if (
                exp.alias in all_declared_aliases
                and exp != all_declared_aliases[exp.alias]
            ):
                raise AliasShadowingException(
                    (
                        f"Shadowing aliases detected for alias: {exp.alias}. "
                        + f"Expressions: {all_declared_aliases[exp.alias]}"
                    )
                )
            else:
                all_declared_aliases[exp.alias] = exp


# A column name like "tags[url]"
NESTED_COL_EXPR_RE = re.compile(r"^([a-zA-Z0-9_\.]+)\[([a-zA-Z0-9_\.:-]+)\]$")


def _parse_subscriptables(query: Query) -> None:
    """
    Turns columns formatted as tags[asd] into SubscriptableReference.
    """
    current_aliases = {exp.alias for exp in query.get_all_expressions() if exp.alias}

    def transform(exp: Expression) -> Expression:
        if not isinstance(exp, Column) or exp.column_name in current_aliases:
            return exp
        match = NESTED_COL_EXPR_RE.match(exp.column_name)
        if match is None:
            # This is not a tag[asd] column.
            return exp
        col_name = match[1]
        key_name = match[2]
        return SubscriptableReference(
            alias=exp.column_name,
            column=Column(None, None, col_name),
            key=Literal(None, key_name),
        )

    query.transform_expressions(transform)


def _apply_column_aliases(query: Query) -> None:
    """
    Applies an alias to all the columns in the query equal to the column
    name unless a column already has one or the alias is already defined.
    In the first case we honor the alias the column already has, in the
    second case the column is a reference to such alias and we do not shadow
    the alias.
    TODO: Inline the full referenced expression if the column is an alias
    reference.
    """
    current_aliases = {exp.alias for exp in query.get_all_expressions() if exp.alias}

    def apply_aliases(exp: Expression) -> Expression:
        if (
            not isinstance(exp, Column)
            or exp.alias
            or exp.column_name in current_aliases
        ):
            return exp
        else:
            return replace(exp, alias=exp.column_name)

    query.transform_expressions(apply_aliases)


def _expand_aliases(query: Query) -> None:
    """
    Recursively expand all the references to aliases in the query. This
    makes life easy to query processors and translators that only have to
    take care of not introducing shadowing (easy to enforce at runtime),
    otherwise aliasing is transparent to them.
    """
    # Pre-inline all nested aliases. This reduces the number of iterations
    # we need to do on the query.
    # Example:
    # {"a": f(x), "x": g(k)} -> {"a": f(g(k)), "x": g(k)}
    aliased_expressions = {
        exp.alias: exp for exp in query.get_all_expressions() if exp.alias is not None
    }
    fully_resolved_aliases = {
        alias: exp.accept(
            AliasExpanderVisitor(aliased_expressions, [], expand_nested=True)
        )
        for alias, exp in aliased_expressions.items()
    }

    visitor = AliasExpanderVisitor(fully_resolved_aliases, [])
    query.transform(visitor)


DEESCAPER_RE = re.compile(r"^`(.+)`$")


def _deescape_aliases(query: Query) -> None:
    """
    The legacy query processing does not escape user declared aliases
    thus aliases like project.name would make the query fail. So Sentry
    started defining pre-escaped aliases like `project.name` to go
    around the problem.
    The AST processing properly escapes aliases thus causing double
    escaping. We need to de-escape them in the AST query to preserve
    backward compatibility as long as the legacy query processing is
    around.
    """

    def deescape(expression: Optional[str]) -> Optional[str]:
        if expression is not None:
            match = DEESCAPER_RE.match(expression)
            if match:
                return match[1]
        return expression

    query.transform_expressions(lambda expr: replace(expr, alias=deescape(expr.alias)))

    query.set_ast_selected_columns(
        [
            replace(s, name=deescape(s.name))
            for s in query.get_selected_columns_from_ast() or []
        ]
    )


class AliasExpanderVisitor(ExpressionVisitor[Expression]):
    """
    Traverses an expression and, when it finds a reference to an alias
    (which is a Column that does not define an alias and such that its
    name is in the lookup table), replaces the column with the expression
    from the lookup table and recursively expand the newly introduced
    expression if requested.

    See visit_column for details on how cycles are managed.
    """

    def __init__(
        self,
        alias_lookup_table: Mapping[str, Expression],
        visited_stack: Sequence[str],
        expand_nested: bool = False,
    ) -> None:
        self.__alias_lookup_table = alias_lookup_table
        # Aliases being visited from the root node till the node
        # we are currently visiting
        self.__visited_stack = visited_stack
        # Recursively expand aliases after resolving them. If True,
        # then, after resolving an alias we visit the result.
        self.__expand_nested = expand_nested

    def visit_literal(self, exp: Literal) -> Expression:
        return exp

    def visit_column(self, exp: Column) -> Expression:
        name = exp.column_name
        if exp.alias is not None or name not in self.__alias_lookup_table:
            return exp

        if name in self.__visited_stack:
            # This means this column is being shadowed (correctly) by an
            # expression that declares an alias equal to the name of the
            # column. Example: f(a) as a.
            #
            # Follows the same Clickhouse approach to cycles:
            # `f(g(z(a))) as a` -> allowed
            # `f(g(z(a) as a) as not_a` -> allowed
            # `f(g(a) as b) as a` -> not allowed
            # `f(a) as b, f(b) as a` -> not allowed
            if self.__visited_stack[-1] != name:
                # We need to reject conditions like `f(g(a) as b) as a`
                # but accept `f(g(a)) as a`.
                # If we are here it means we already found, in this nested
                # expression, an alias declared that has the same name of
                # the column we are visiting (shadowing). So the previously
                # visited alias must be on top of the stack, to be valid.
                # In `f(g(a) as b) as a`, instead, b would be on top of the
                # stack instead of a.
                raise CyclicAliasException(
                    f"Cyclic aliases {name} resolves to {self.__alias_lookup_table[name]}"
                )
            return exp

        if self.__expand_nested:
            # The expanded expression may contain more alias references to expand.
            return self.__alias_lookup_table[name].accept(self)
        else:
            return self.__alias_lookup_table[name]

    def __append_alias(self, alias: Optional[str]) -> Sequence[str]:
        return (
            [*self.__visited_stack, alias]
            if alias is not None
            else self.__visited_stack
        )

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> Expression:
        expanded_column = exp.column.accept(
            AliasExpanderVisitor(
                self.__alias_lookup_table,
                self.__append_alias(exp.alias),
                self.__expand_nested,
            )
        )
        assert isinstance(
            expanded_column, Column
        ), "A subscriptable column cannot be resolved to anything other than a column"
        return replace(
            exp,
            column=expanded_column,
            key=exp.key.accept(
                AliasExpanderVisitor(
                    self.__alias_lookup_table,
                    self.__append_alias(exp.alias),
                    self.__expand_nested,
                )
            ),
        )

    def __visit_sequence(
        self, alias: Optional[str], parameters: Sequence[Expression]
    ) -> Tuple[Expression, ...]:
        return tuple(
            p.accept(
                AliasExpanderVisitor(
                    self.__alias_lookup_table,
                    self.__append_alias(alias),
                    self.__expand_nested,
                )
            )
            for p in parameters
        )

    def visit_function_call(self, exp: FunctionCall) -> Expression:
        return replace(exp, parameters=self.__visit_sequence(exp.alias, exp.parameters))

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> Expression:
        return replace(
            exp,
            internal_function=exp.internal_function.accept(
                AliasExpanderVisitor(
                    self.__alias_lookup_table,
                    self.__append_alias(exp.alias),
                    self.__expand_nested,
                )
            ),
            parameters=self.__visit_sequence(exp.alias, exp.parameters),
        )

    def visit_argument(self, exp: Argument) -> Expression:
        return exp

    def visit_lambda(self, exp: Lambda) -> Expression:
        return replace(
            exp,
            transformation=exp.transformation.accept(
                AliasExpanderVisitor(
                    self.__alias_lookup_table,
                    self.__append_alias(exp.alias),
                    self.__expand_nested,
                )
            ),
        )
