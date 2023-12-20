from __future__ import annotations

from dataclasses import replace
from typing import Generator, Mapping

from snuba.query import ProcessableQuery, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    combine_and_conditions,
    get_first_level_and_conditions,
)
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinNode,
    JoinVisitor,
)
from snuba.query.data_source.multi import MultiQuery
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, Expression
from snuba.query.joins.classifier import (
    AliasGenerator,
    BranchCutter,
    SubExpression,
    SubqueryExpression,
)
from snuba.query.logical import Query as LogicalQuery


class SubqueryDraft:
    """
    Data structure used to progressively build a subquery.
    """

    def __init__(self, data_source: Entity) -> None:
        self.__data_source = data_source
        self.__selected_expressions: set[SelectedExpression] = set()
        self.__conditions: list[Expression] = []
        self.__granularity: int | None = None

    def add_select_expression(self, expression: SelectedExpression) -> None:
        self.__selected_expressions.add(expression)

    def add_condition(self, condition: Expression) -> None:
        self.__conditions.append(condition)

    def set_granularity(self, granularity: int | None) -> None:
        self.__granularity = granularity

    def build_query(self) -> ProcessableQuery[Entity]:
        return LogicalQuery(
            from_clause=self.__data_source,
            selected_columns=list(
                sorted(
                    self.__selected_expressions,
                    key=lambda selected: selected.name or "",
                )
            ),
            condition=combine_and_conditions(self.__conditions)
            if self.__conditions
            else None,
            granularity=self.__granularity,
        )


def aliasify_column(col_name: str) -> str:
    return f"_snuba_{col_name}"


class SubqueriesInitializer(JoinVisitor[Mapping[str, SubqueryDraft], Entity]):
    """
    Visits a join clause and generates SubqueryDraft instances
    for each node. It adds the columns needed for the ON clause
    to the selected clause of the subqueries as well.
    """

    def visit_individual_node(
        self, node: IndividualNode[Entity]
    ) -> Mapping[str, SubqueryDraft]:
        entity = node.data_source
        assert isinstance(entity, Entity)
        return {node.alias: SubqueryDraft(entity)}

    def visit_join_clause(
        self, node: JoinClause[Entity]
    ) -> Mapping[str, SubqueryDraft]:
        combined = {**node.left_node.accept(self), **node.right_node.accept(self)}
        for condition in node.keys:
            combined[condition.left.table_alias].add_select_expression(
                SelectedExpression(
                    # Setting a name for the selected columns in a subquery
                    # is not terribly useful, as this name would not
                    # be used anywhere.
                    # The external query references subquery columns by
                    # their aliases as this is what Clickhouse will do.
                    aliasify_column(condition.left.column),
                    Column(
                        aliasify_column(condition.left.column),
                        None,
                        condition.left.column,
                    ),
                )
            )
            combined[condition.right.table_alias].add_select_expression(
                SelectedExpression(
                    aliasify_column(condition.right.column),
                    Column(
                        aliasify_column(condition.right.column),
                        None,
                        condition.right.column,
                    ),
                )
            )
        return combined


class SubqueriesReplacer(JoinVisitor[JoinNode[Entity], Entity]):
    """
    Replaces the entities in the original joins with the relevant
    SubqueryDraft objects.
    """

    def __init__(self, subqueries: Mapping[str, SubqueryDraft]) -> None:
        self.__subqueries = subqueries

    def visit_individual_node(
        self, node: IndividualNode[Entity]
    ) -> IndividualNode[Entity]:
        return IndividualNode(node.alias, self.__subqueries[node.alias].build_query())

    def visit_join_clause(self, node: JoinClause[Entity]) -> JoinClause[Entity]:
        """
        This tweaks the names of the columns in the ON clause as they
        cannot reference the entity fields directly (as in the original
        query) but they have to reference the mangled aliases generated
        by SubqueriesInitializer.

        This is needed because Clickhouse does not know about the
        names of the SelectedExpression nodes in the AST. It is only
        aware of aliases as a way to reference expressions from the
        external query to the subqueries.
        """
        left = node.left_node.accept(self)
        right = self.visit_individual_node(node.right_node)
        keys = [
            JoinCondition(
                left=JoinConditionExpression(
                    k.left.table_alias, aliasify_column(k.left.column)
                ),
                right=JoinConditionExpression(
                    k.right.table_alias, aliasify_column(k.right.column)
                ),
            )
            for k in node.keys
        ]
        return JoinClause(left, right, keys, node.join_type, node.join_modifier)


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


def _alias_generator() -> Generator[str, None, None]:
    i = 0
    while True:
        i += 1
        yield f"_snuba_gen_{i}"


def generate_subqueries(query: CompositeQuery[Entity]) -> None:
    """
    Generates correct subqueries for each of the entities referenced in
    a join query, and pushes down all expressions that can be executed
    in the subquery.

    Columns in the select clause of the subqueries are referenced
    by providing them a mangled alias that is referenced in the external
    query.

    ```
    SELECT e.a, f(g.b) FROM Events e INNER JOIN Groups g ON ...
    ```

    becomes

    ```
    SELECT e._snuba_a, g._snuba_b
    FROM (
        SELECT a as _snuba_a
        FROM events
    ) e INNER JOIN (
        SELECT f(b) as _snuba_b
        FROM groups
    ) g ON ....
    ```

    Conditions are treated differently compared to other expressions. If
    a condition is entirely contained in a single subquery, we push it
    down entirely in the condition clause of the subquery and remove it
    from the main query entirely.
    """

    from_clause = query.get_from_clause()
    if isinstance(from_clause, CompositeQuery):
        generate_subqueries(from_clause)
        return
    elif isinstance(from_clause, ProcessableQuery):
        return
    elif isinstance(from_clause, MultiQuery):
        return

    # Now this has to be a join, so we can work with it.
    subqueries = from_clause.accept(SubqueriesInitializer())

    alias_generator = _alias_generator()
    query.set_ast_selected_columns(
        [
            SelectedExpression(
                name=s.name,
                expression=_process_root(s.expression, subqueries, alias_generator),
            )
            for s in query.get_selected_columns()
        ]
    )

    array_join = query.get_arrayjoin()
    if array_join is not None:
        query.set_arrayjoin(
            [_process_root(el, subqueries, alias_generator) for el in array_join]
        )

    ast_condition = query.get_condition()
    if ast_condition is not None:
        main_conditions = []
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
                main_conditions.append(
                    _push_down_branches(subexpression, subqueries, alias_generator)
                )

        if main_conditions:
            query.set_ast_condition(combine_and_conditions(main_conditions))
        else:
            query.set_ast_condition(None)

    for s in subqueries.values():
        s.set_granularity(query.get_granularity())

    # TODO: push down the group by when it is the same as the join key.
    query.set_ast_groupby(
        [_process_root(e, subqueries, alias_generator) for e in query.get_groupby()]
    )

    having = query.get_having()
    if having is not None:
        query.set_ast_having(
            combine_and_conditions(
                [
                    _process_root(c, subqueries, alias_generator)
                    for c in get_first_level_and_conditions(having)
                ]
            )
        )

    query.set_ast_orderby(
        [
            replace(
                orderby,
                expression=_process_root(
                    orderby.expression, subqueries, alias_generator
                ),
            )
            for orderby in query.get_orderby()
        ]
    )

    limitby = query.get_limitby()
    if limitby is not None:
        query.set_limitby(
            replace(
                limitby,
                columns=[
                    _process_root(
                        column,
                        subqueries,
                        alias_generator,
                    )
                    for column in limitby.columns
                ],
            )
        )

    query.set_from_clause(SubqueriesReplacer(subqueries).visit_join_clause(from_clause))
