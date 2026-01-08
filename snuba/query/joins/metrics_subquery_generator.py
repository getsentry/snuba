from __future__ import annotations

from typing import Generator, Mapping

from snuba.query import ProcessableQuery, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import get_first_level_and_conditions
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Expression
from snuba.query.joins.classifier import (
    AggregateBranchCutter,
    AliasGenerator,
    BranchCutter,
    SubExpression,
    SubqueryExpression,
)
from snuba.query.joins.subquery_generator import (
    SubqueriesInitializer,
    SubqueriesReplacer,
    SubqueryDraft,
    generate_subqueries,
)


class MetricsSubqueriesInitializer(SubqueriesInitializer):
    def visit_join_clause(self, node: JoinClause[Entity]) -> Mapping[str, SubqueryDraft]:
        combined = {**node.left_node.accept(self), **node.right_node.accept(self)}
        return combined


def _process_aggregate_value(
    expression: Expression,
    subqueries: Mapping[str, SubqueryDraft],
    alias_generator: AliasGenerator,
) -> Expression:
    """
    Takes an aggregate value function and pushes down the inner aggregate functions
    to the subqueries.
    """
    subexpressions = expression.accept(AggregateBranchCutter(alias_generator))
    return _push_down_branches(subexpressions, subqueries, alias_generator)


def _process_root(
    expression: Expression,
    subqueries: Mapping[str, SubqueryDraft],
    alias_generator: AliasGenerator,
) -> Expression:
    """
    Takes a root expression in the main query, runs the branch cutter
    and pushes down the subexpressions.
    """
    subexpressions = expression.accept(BranchCutter(alias_generator))
    return _push_down_branches(subexpressions, subqueries, alias_generator)


def _push_down_branches(
    subexpressions: SubExpression,
    subqueries: Mapping[str, SubqueryDraft],
    alias_generator: AliasGenerator,
) -> Expression:
    """
    Pushes the branches of a SubExpression into subqueries and
    returns the main expression for the main query.
    """
    cut_subexpression = subexpressions.cut_branch(alias_generator)
    for entity_alias, branches in cut_subexpression.cut_branches.items():
        for branch in branches:
            subqueries[entity_alias].add_select_expression(
                SelectedExpression(name=branch.alias, expression=branch)
            )

    return cut_subexpression.main_expression


def _push_down_conditions(
    subexpressions: SubExpression,
    subqueries: Mapping[str, SubqueryDraft],
    alias_generator: AliasGenerator,
) -> None:
    """ """
    cut_subexpression = subexpressions.cut_branch(alias_generator)
    for entity_alias, branches in cut_subexpression.cut_branches.items():
        for branch in branches:
            subqueries[entity_alias].add_condition(branch)


def _process_root_groupby(
    query: CompositeQuery[Entity],
    expression: Expression,
    subqueries: Mapping[str, SubqueryDraft],
    alias_generator: AliasGenerator,
) -> Expression:
    """
    Takes a root expression in the main query, runs the branch cutter
    and pushes down the subexpressions.
    """
    subexpressions = expression.accept(BranchCutter(alias_generator))
    return _push_down_groupby_branches(query, subexpressions, subqueries, alias_generator)


def _push_down_groupby_branches(
    query: CompositeQuery[Entity],
    subexpressions: SubExpression,
    subqueries: Mapping[str, SubqueryDraft],
    alias_generator: AliasGenerator,
) -> Expression:
    """
    Pushes the branches of a SubExpression into subqueries and
    returns the main expression for the main query.
    """
    cut_subexpression = subexpressions.cut_branch(alias_generator)
    for entity_alias, branches in cut_subexpression.cut_branches.items():
        for branch in branches:
            if query.has_totals():
                subqueries[entity_alias].set_totals(query.has_totals())
            subqueries[entity_alias].add_groupby_expression(branch)

    return cut_subexpression.main_expression


def _alias_generator() -> Generator[str, None, None]:
    i = 0
    while True:
        i += 1
        yield f"_snuba_gen_{i}"


def generate_metrics_subqueries(query: CompositeQuery[Entity]) -> None:
    """
    Generates correct subqueries for each of the entities referenced in
    a join query, and pushes down all expressions that can be executed
    in the subquery.

    Columns in the select clause of the subqueries are referenced
    by providing them a mangled alias that is referenced in the external
    query.

    MQL join queries follow a more strict format that other composite queries,
    so can be handled more explicitly.

    SELECT clause

    MQL queries always have the same things in the SELECT clause, with specific push down logic.

    - An aggregate value function (required)
    This will be a FunctionCall that will have paramters that are functions of the form
    f(a), where f is an aggregate function and a is the "value" column.
    In this case, the top level function should stay, but the inner aggregates f(a) should
    be pushed down to the subqueries, not just the "value" column.

    - Time columns (optional)
    This will be a FunctionCall on the timestamp column that generates the desired
    time intervals. This column will be missing for totals queries. There will be a time column
    for each entity, so each should be pushed in its entirety to the subquery.

    - Columns used in the group by (optional)
    Group by columns can be a SubscriptableReference to the tags column, or a concrete
    column like `project_id`. These should be pushed in its entirety to the subquery.

    WHERE clause

    There will never be conditions that cross entities, so each top level AND condition should
    be pushed down to the appropriate subquery.

    GROUP BY clause

    Crucially, the group by columns are also qualified and should also be pushed down to the
    subqueries, so the join is happening on grouped results instead of the full result set.

    ```
    SELECT g(f(d0.a), f(d1.b)), t() as d0.time, t() as d1.time, d0.gb, d1.gb
    FROM d0 INNER JOIN d1 ON ...
    WHERE d0.m = 1 AND d1.m = 2
    GROUP BY d0.time, d1.time, d0.gb, d1.gb
    ORDER BY ...
    ```

    becomes

    ```
    SELECT g(d0._snuba_a, d1._snuba_b), d0._snuba_time, d1._snuba_time, d0._snuba_gb, d1._snuba_gb
    FROM (
        SELECT f(a) as _snuba_a, t() as _snuba_time, gb as _snuba_gb
        FROM ...
        WHERE m = 1
        GROUP BY _snuba_time, _snuba_gb
    ) d0 INNER JOIN (
        SELECT g(b) as _snuba_b, t() as _snuba_time, gb as _snuba_gb
        FROM ...
        WHERE m = 2
        GROUP BY _snuba_time, _snuba_gb
    ) d1 ON ...
    ORDER BY ...
    ```

    MQL queries won't have any other clauses than this, so this processor doesn't need to handle those cases.
    """
    from_clause = query.get_from_clause()
    if isinstance(from_clause, CompositeQuery):
        generate_subqueries(from_clause)
        return
    elif isinstance(from_clause, ProcessableQuery):
        return

    # Now this has to be a join, so we can work with it.
    subqueries = from_clause.accept(MetricsSubqueriesInitializer())
    alias_generator = _alias_generator()

    selected_columns = []
    for s in query.get_selected_columns():
        if s.name == "aggregate_value":
            selected_columns.append(
                SelectedExpression(
                    name=s.name,
                    expression=_process_aggregate_value(s.expression, subqueries, alias_generator),
                )
            )
        else:
            selected_columns.append(
                SelectedExpression(
                    name=s.name,
                    expression=_process_root(s.expression, subqueries, alias_generator),
                )
            )

    query.set_ast_selected_columns(selected_columns)
    ast_condition = query.get_condition()
    if ast_condition is not None:
        for c in get_first_level_and_conditions(ast_condition):
            subexpression = c.accept(BranchCutter(alias_generator))
            if isinstance(subexpression, SubqueryExpression):
                # The expression is entirely contained in a single subquery
                # after we tried to cut subquery branches with the
                # BranchCutter visitor.
                # so push down the entire condition and remove it from
                # the main query.
                subqueries[subexpression.subquery_alias].add_condition(
                    subexpression.main_expression
                )
            else:
                # This condition has references to multiple subqueries.
                # We cannot push down the condition. We push down the
                # branches into the select clauses and we reference them
                # from the main query condition.
                _push_down_conditions(subexpression, subqueries, alias_generator)

        query.set_ast_condition(None)

    for e in query.get_groupby():
        _process_root_groupby(query, e, subqueries, alias_generator)

    # Since groupbys are pushed down, we don't need them in the outer query.
    query.set_ast_groupby([])

    query.set_from_clause(SubqueriesReplacer(subqueries).visit_join_clause(from_clause))
