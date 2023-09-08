from dataclasses import replace
from typing import Mapping, MutableMapping, Optional, Sequence, Tuple, Union

from snuba import environment
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity as QueryEntity
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
from snuba.query.logical import Query
from snuba.query.parser.exceptions import AliasShadowingException, CyclicAliasException
from snuba.utils.constants import NESTED_COL_EXPR_RE
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "parser")


def validate_aliases(query: Union[CompositeQuery[QueryEntity], Query]) -> None:
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
                    ),
                    should_report=False,
                )
            else:
                all_declared_aliases[exp.alias] = exp


def parse_subscriptables(query: Union[CompositeQuery[QueryEntity], Query]) -> None:
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
            alias=exp.alias or exp.column_name,
            column=Column(None, None, col_name),
            key=Literal(None, key_name),
        )

    query.transform_expressions(transform)


def apply_column_aliases(query: Union[CompositeQuery[QueryEntity], Query]) -> None:
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


def expand_aliases(query: Union[CompositeQuery[QueryEntity], Query]) -> None:
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
                    f"Cyclic aliases {name} resolves to {self.__alias_lookup_table[name]}",
                    should_report=False,
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
