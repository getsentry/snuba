from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from functools import partial
from typing import (
    Callable,
    Generator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
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
from snuba.query.functions import is_aggregation_function

AliasGenerator = Generator[str, None, None]


# This is a workaround for a mypy bug, found here: https://github.com/python/mypy/issues/5374
@dataclass(frozen=True)
class _SubExpression:
    main_expression: Expression


class SubExpression(_SubExpression, ABC):
    """
    Data structure maintained when visiting an Expression in the query.
    This keeps track of the main branch (the node we are visiting) as
    well as all the branches we cut that were under the main expression.
    Those cut branches should be pushed down into subqueries.
    """

    @abstractmethod
    def cut_branch(self, alias_generator: AliasGenerator) -> MainQueryExpression:
        """
        Cut the expression tree dividing the subtree/s that need to be
        pushed down into the subqueries from the expression that must
        remain in the main query and that references the subtrees.
        """

        raise NotImplementedError


@dataclass(frozen=True)
class MainQueryExpression(SubExpression):
    """
    A SubExpression that contains subtrees to be pushed down and a main
    expression for the main query.
    """

    cut_branches: Mapping[str, Set[Expression]]

    def cut_branch(self, alias_generator: AliasGenerator) -> MainQueryExpression:
        return self


@dataclass(frozen=True)
class SubqueryExpression(SubExpression):
    """
    A SubExpression that can be entirely pushed into a subquery since
    it references columns from one subquery only.
    """

    subquery_alias: str

    def cut_branch(self, alias_generator: AliasGenerator) -> MainQueryExpression:
        """
        This Returns a simple column that references the expression in
        the subquery and cut the entire expression tree.
        """
        alias = self.main_expression.alias or next(alias_generator)
        branch = (
            self.main_expression
            if self.main_expression.alias
            else replace(self.main_expression, alias=alias)
        )
        return MainQueryExpression(
            # The main expression is replacing the cut branch in the main
            # query, while the branch we cut will go into a subquery, thus
            # will be out of the namespace.
            # The replacing expression preserves the alias of the branch.
            # This allows external query (in case the main query is itself
            # a subquery) to still be able to reference the expression we
            # pushed down.
            #
            # Example:
            # ```
            # SELECT avg(id) FROM (SELECT a.id FROM ... join ...)
            # ```
            # after branch cutting becomes:
            # ```
            # SELECT avg(_snuba_id) FROM
            #    SELECT a._snuba_a.id as _snuba_id
            #        FROM
            #           (SELECT id as _snuba_a.id FROM ...) a
            #           JOIN
            #           (SELECT .....) b
            # ```
            # When we cut the branch containing id, it is replaced by the
            # column `a._snuba_a.id`, which is still referenced from
            # the `avg` function thus it needs to preserve the original
            # mangled alias it had after parsing.
            main_expression=Column(alias, self.subquery_alias, alias),
            cut_branches={self.subquery_alias: {branch}},
        )


@dataclass(frozen=True)
class UnclassifiedExpression(SubExpression):
    """
    A SubExpression that does not reference any column, thus can stay
    anywhere.
    """

    def cut_branch(self, alias_generator: AliasGenerator) -> MainQueryExpression:
        return MainQueryExpression(main_expression=self.main_expression, cut_branches={})


def _merge_subexpressions(
    builder: Callable[[List[Expression]], Expression],
    sub_expressions: Sequence[SubExpression],
    alias_generator: AliasGenerator,
) -> SubExpression:
    """
    Merges multiple Subexpressions into one depending on the type of the
    subexpressions. This is generally used to address parameters of a
    function and cut the relevant branches.
    """

    subqueries = set()
    require_branch_cut = False
    for visited in sub_expressions:
        if isinstance(visited, SubqueryExpression):
            subqueries.add(visited.subquery_alias)
        elif isinstance(visited, MainQueryExpression):
            require_branch_cut = True

    if len(subqueries) > 1:
        require_branch_cut = True

    if not require_branch_cut:
        if not subqueries:
            # All parameters are not classified. This function is also
            # not classified.
            return UnclassifiedExpression(builder([v.main_expression for v in sub_expressions]))
        else:
            # All parameters are either not classified or in a single
            # subquery. This function is also referencing that subquery
            # only.
            return SubqueryExpression(
                builder([v.main_expression for v in sub_expressions]),
                subquery_alias=subqueries.pop(),
            )
    else:
        return _merge_and_cut(builder, sub_expressions, alias_generator)


def _merge_and_cut(
    builder: Callable[[List[Expression]], Expression],
    sub_expressions: Sequence[SubExpression],
    alias_generator: AliasGenerator,
) -> SubExpression:
    cut_branches: MutableMapping[str, Set[Expression]] = {}
    parameters = []
    for v in sub_expressions:
        cut = v.cut_branch(alias_generator)
        parameters.append(cut.main_expression)
        for entity, branches in cut.cut_branches.items():
            branch_set = cut_branches.setdefault(entity, branches)
            branch_set |= branches

    return MainQueryExpression(builder(parameters), cut_branches)


class BranchCutter(ExpressionVisitor[SubExpression]):
    """
    Visits an expression and finds which subtrees can be pushed
    down to the subqueries (cut the branch), and which expressions
    must stay in the main query.

    It produces an instance of SubExpression above which includes
    what remains of the main expression to fit in the main query
    (with references to the subtrees in the subqueries) together
    with the branches that were cut to be pushed down.

    Each time it cuts a branch it replaces it with a Column expression
    that references an expression in the select clause of a subquery.
    This column does not have an alias to avoid shadowing in the
    main query.

    Example:
    ```
    f(
        errors.a AS _snuba_a,
        g(groups.b AS _snuba_b) AS _snuba_g,
    ) AS _snuba_f
    ```
    becomes:
    main query:
    `f(errors._snuba_a, groups._snuba_g) as _snuba_f`
    cut branches:
    `a AS _snuba_a`
    `g(b as _snuba_b) AS _snuba_g`
    """

    def __init__(self, alias_generator: AliasGenerator) -> None:
        self.__alias_generator = alias_generator

    def visit_literal(self, exp: Literal) -> SubExpression:
        return UnclassifiedExpression(exp)

    def visit_column(self, exp: Column) -> SubExpression:
        assert exp.table_name, f"Invalid column expression in join: {exp}. Missing table alias"
        return SubqueryExpression(Column(exp.alias, None, exp.column_name), exp.table_name)

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> SubExpression:
        assert exp.column.table_name, (
            f"Invalid column expression in join: {exp}. Missing table alias"
        )
        return SubqueryExpression(
            main_expression=SubscriptableReference(
                exp.alias,
                Column(exp.column.alias, None, exp.column.column_name),
                Literal(exp.key.alias, exp.key.value),
            ),
            subquery_alias=exp.column.table_name,
        )

    def visit_function_call(self, exp: FunctionCall) -> SubExpression:
        def builder(
            alias: Optional[str], func_name: str, params: Sequence[Expression]
        ) -> FunctionCall:
            return FunctionCall(alias, func_name, tuple(params))

        visited_params = [p.accept(self) for p in exp.parameters]
        # In the general case we cannot push down aggregation
        # expressions (the call to the aggergation function). This
        # is because the aggregation function needs to be where the
        # relevant group by is.
        # A further step will be to push down the group by as well
        # when possible, and that will allow us to push down those
        # aggregation functions.
        #
        # TODO: Push down group by when possible.
        if is_aggregation_function(exp.function_name):
            return _merge_and_cut(
                builder=partial(builder, exp.alias, exp.function_name),
                sub_expressions=visited_params,
                alias_generator=self.__alias_generator,
            )

        return _merge_subexpressions(
            builder=partial(builder, exp.alias, exp.function_name),
            sub_expressions=visited_params,
            alias_generator=self.__alias_generator,
        )

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> SubExpression:
        def builder(alias: Optional[str], params: List[Expression]) -> Expression:
            # The first element in the sequence is the inner function.
            # Unfortunately I could not find a better way to reuse this
            # between FunctionCall and CurriedFunctionCall.
            inner_function = params.pop(0)
            assert isinstance(inner_function, FunctionCall)
            return CurriedFunctionCall(alias, inner_function, tuple(params))

        visited_inner = exp.internal_function.accept(self)
        visited_params = [p.accept(self) for p in exp.parameters]
        return _merge_subexpressions(
            builder=partial(builder, exp.alias),
            sub_expressions=[visited_inner, *visited_params],
            alias_generator=self.__alias_generator,
        )

    def visit_argument(self, exp: Argument) -> SubExpression:
        return UnclassifiedExpression(exp)

    def visit_lambda(self, exp: Lambda) -> SubExpression:
        transformed = exp.transformation.accept(self)
        return replace(
            transformed,
            main_expression=Lambda(exp.alias, exp.parameters, transformed.main_expression),
        )

    def visit_dangerous_raw_sql(self, exp: DangerousRawSQL) -> SubExpression:
        return UnclassifiedExpression(exp)

    def visit_json_path(self, exp: JsonPath) -> SubExpression:
        visited_base = exp.base.accept(self)
        return replace(
            visited_base,
            main_expression=replace(exp, base=visited_base.main_expression),
        )


class AggregateBranchCutter(BranchCutter):
    """
    Metrics JOIN queries do push down the group by columns, and so they don't need to avoid pushing down aggregate
    functions. In fact the opposite is true, the query will fail if the aggregate functions are not pushed down.
    """

    def __init__(self, alias_generator: AliasGenerator) -> None:
        self.__alias_generator = alias_generator
        super().__init__(alias_generator)

    def visit_function_call(self, exp: FunctionCall) -> SubExpression:
        def builder(
            alias: Optional[str], func_name: str, params: Sequence[Expression]
        ) -> FunctionCall:
            return FunctionCall(alias, func_name, tuple(params))

        visited_params = [p.accept(self) for p in exp.parameters]
        return _merge_subexpressions(
            builder=partial(builder, exp.alias, exp.function_name),
            sub_expressions=visited_params,
            alias_generator=self.__alias_generator,
        )
