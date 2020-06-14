import logging
import re
from collections import defaultdict
from dataclasses import replace
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

from snuba import state
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
from snuba.query.logical import OrderBy, OrderByDirection, Query
from snuba.query.parser.conditions import parse_conditions_to_expr
from snuba.query.parser.expressions import parse_aggregation, parse_expression
from snuba.util import is_function, to_list, tuplify

logger = logging.getLogger(__name__)


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
    try:
        query = _parse_query_impl(body, dataset)
        # These are the post processing phases
        _validate_aliases(query)
        _parse_subscriptables(query)
        _apply_column_aliases(query)
        _expand_aliases(query)
        # WARNING: These steps above assume table resolution did not happen
        # yet. If it is put earlier than here (unlikely), we need to adapt them.
        return query
    except Exception as e:
        # During the development there is no need to fail Snuba queries if the parser
        # has an issue, anyway the production query is ran based on the old query
        # representation.
        # Once we will be actually using the ast to build the Clickhouse query
        # this try/except block will disappear.
        enforce_validity = state.get_config("query_parsing_enforce_validity", 0)
        if enforce_validity:
            raise e
        else:
            logger.warning("Failed to parse query", exc_info=True)
            return Query(body, None)


def _parse_query_impl(body: MutableMapping[str, Any], dataset: Dataset) -> Query:
    aggregate_exprs = []
    for aggregation in body.get("aggregations", []):
        assert isinstance(aggregation, (list, tuple))
        aggregation_function = aggregation[0]
        column_expr = aggregation[1]
        column_expr = column_expr if column_expr else []
        alias = aggregation[2]
        alias = alias if alias else None

        aggregate_exprs.append(
            parse_aggregation(aggregation_function, column_expr, alias)
        )

    groupby_exprs = [
        parse_expression(tuplify(group_by))
        for group_by in to_list(body.get("groupby", []))
    ]
    select_exprs = [
        parse_expression(tuplify(select)) for select in body.get("selected_columns", [])
    ]

    selected_cols = groupby_exprs + aggregate_exprs + select_exprs

    arrayjoin = body.get("arrayjoin")
    if arrayjoin:
        array_join_expr: Optional[Expression] = parse_expression(body["arrayjoin"])
    else:
        array_join_expr = None

    where_expr = parse_conditions_to_expr(
        body.get("conditions", []), dataset, arrayjoin
    )
    having_expr = parse_conditions_to_expr(body.get("having", []), dataset, arrayjoin)

    orderby_exprs = []
    for orderby in to_list(body.get("orderby", [])):
        if isinstance(orderby, str):
            match = NEGATE_RE.match(orderby)
            assert match is not None, f"Invalid Order By clause {orderby}"
            direction, col = match.groups()
            orderby = col
        elif is_function(orderby):
            match = NEGATE_RE.match(orderby[0])
            assert match is not None, f"Invalid Order By clause {orderby}"
            direction, col = match.groups()
            orderby = [col] + orderby[1:]
        else:
            raise ValueError(f"Invalid Order By clause {orderby}")
        orderby_parsed = parse_expression(tuplify(orderby))
        orderby_exprs.append(
            OrderBy(
                OrderByDirection.DESC if direction == "-" else OrderByDirection.ASC,
                orderby_parsed,
            )
        )

    return Query(
        body,
        None,
        selected_columns=selected_cols,
        array_join=array_join_expr,
        condition=where_expr,
        groupby=groupby_exprs,
        having=having_expr,
        order_by=orderby_exprs,
    )


def _validate_aliases(query: Query) -> None:
    """
    Ensures that no alias has been defined multiple times for different
    expressions in the query. Thus rejecting queries with shadowing.
    """
    all_declared_aliases: Mapping[str, Set[Expression]] = defaultdict(set)
    for exp in query.get_all_expressions():
        # TODO: Make it impossible to assign empty string as an alias.
        if exp.alias:
            all_declared_aliases[exp.alias].add(exp)
            new_exps = all_declared_aliases[exp.alias]
            if len(new_exps) > 1:
                raise ValueError(
                    f"Shadowing aliases detected for alias: {exp.alias}. Expressions: {new_exps}"
                )


# A column name like "tags[url]"
NESTED_COL_EXPR_RE = re.compile(r"^([a-zA-Z0-9_\.]+)\[([a-zA-Z0-9_\.:-]+)\]$")


def _parse_subscriptables(query: Query) -> None:
    """
    Turns columns formatted as tags[asd] into SubscriptableReference.
    """
    current_aliases = {exp.alias for exp in query.get_all_expressions() if exp.alias}

    def transform(exp: Expression) -> Expression:
        if not isinstance(exp, Column):
            return exp
        match = NESTED_COL_EXPR_RE.match(exp.column_name)
        if match is None or exp.column_name in current_aliases:
            # Either this is not a tag[asd] column or there is actually
            # somewhere in the Query, an expression that declares the
            # alias tags[asd]. So do not redefine it.
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
    query.set_ast_selected_columns(
        [e.accept(visitor) for e in (query.get_selected_columns_from_ast() or [])]
    )
    arrayjoin = query.get_arrayjoin_from_ast()
    if arrayjoin is not None:
        query.set_ast_arrayjoin(arrayjoin.accept(visitor))
    condition = query.get_condition_from_ast()
    if condition is not None:
        query.set_ast_condition(condition.accept(visitor))
    query.set_ast_groupby(
        [e.accept(visitor) for e in (query.get_groupby_from_ast() or [])]
    )
    having = query.get_having_from_ast()
    if having is not None:
        query.set_ast_having(having.accept(visitor))
    query.set_ast_orderby(
        list(
            map(
                lambda clause: replace(
                    clause, expression=clause.expression.accept(visitor)
                ),
                query.get_orderby_from_ast(),
            )
        )
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
            # f(g(z(a))) as a -> allowed
            # f(g(a) as b) as a -> not allowed
            # f(a) as b, f(b) as a -> not allowed
            assert (
                len(self.__visited_stack) == 1
            ), f"Cyclic aliases {name} resolves to {self.__alias_lookup_table[name]}"
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
