from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from functools import partial
from typing import (
    Callable,
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
    Expression,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)


@dataclass(frozen=True)
class SubExpression(ABC):
    """
    Data structure maintained when visiting an Expression in the query.
    This keeps track of the main branch (the node we are visiting) as
    well as all the branches we cut that were under the main expression.
    Those cut branches should be pushed down into subqueries.
    """

    main_expression: Expression

    @abstractmethod
    def cut_branch(self) -> MainQueryExpression:
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

    def cut_branch(self) -> MainQueryExpression:
        return self


@dataclass(frozen=True)
class SubqueryExpression(SubExpression):
    """
    A SubExpression that can be entirely pushed into a subquery since
    it references columns from one subquery only.
    """

    subquery_alias: str

    def cut_branch(self) -> MainQueryExpression:
        """
        This Returns a simple column that references the expression in
        the subquery and cut the entire expression tree.
        """
        assert (
            self.main_expression.alias
        ), f"Invalid expression in join query {self.main_expression}. Missing alias"
        return MainQueryExpression(
            main_expression=Column(
                None, self.subquery_alias, self.main_expression.alias
            ),
            cut_branches={self.subquery_alias: {self.main_expression}},
        )


@dataclass(frozen=True)
class UnclassifiedExpression(SubExpression):
    """
    A SubExpression that does not reference any column, thus can stay
    anywhere.
    """

    def cut_branch(self) -> MainQueryExpression:
        return MainQueryExpression(
            main_expression=self.main_expression, cut_branches={}
        )


def _merge_subexpressions(
    builder: Callable[[List[Expression]], Expression],
    sub_expressions: Sequence[SubExpression],
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
            return UnclassifiedExpression(
                builder([v.main_expression for v in sub_expressions])
            )
        else:
            # All parameters are either not classified or in a single
            # subquery. This function is also referencing that subquery
            # only.
            return SubqueryExpression(
                builder([v.main_expression for v in sub_expressions]),
                subquery_alias=subqueries.pop(),
            )
    else:
        cut_branches: MutableMapping[str, Set[Expression]] = {}
        parameters = []
        for v in sub_expressions:
            cut = v.cut_branch()
            parameters.append(cut.main_expression)
            for entity, branches in cut.cut_branches.items():
                cut_branches.setdefault(entity, branches).add(*branches)
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

    def visit_literal(self, exp: Literal) -> SubExpression:
        return UnclassifiedExpression(exp)

    def visit_column(self, exp: Column) -> SubExpression:
        assert (
            exp.table_name
        ), f"Invalid column expression in join: {exp}. Missing table alias"
        return SubqueryExpression(
            Column(exp.alias, None, exp.column_name), exp.table_name
        )

    def visit_subscriptable_reference(
        self, exp: SubscriptableReference
    ) -> SubExpression:
        assert (
            exp.column.table_name
        ), f"Invalid column expression in join: {exp}. Missing table alias"
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
        # TODO: Ensure we cut the branch when we encounter aggregate functions.
        return _merge_subexpressions(
            builder=partial(builder, exp.alias, exp.function_name),
            sub_expressions=visited_params,
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
        )

    def visit_argument(self, exp: Argument) -> SubExpression:
        return UnclassifiedExpression(exp)

    def visit_lambda(self, exp: Lambda) -> SubExpression:
        transformed = exp.transformation.accept(self)
        return replace(
            transformed,
            main_expression=Lambda(
                exp.alias, exp.parameters, transformed.main_expression
            ),
        )
