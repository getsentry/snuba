from typing import Mapping, Set

from dataclasses import replace
from snuba.query import ProcessableQuery, SelectedExpression
from snuba.query import expressions
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinNode,
    JoinVisitor,
)
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, Expression
from snuba.query.logical import Query as LogicalQuery
from snuba.query.joins.classifier import BranchCutter
from snuba.query.conditions import (
    get_first_level_and_conditions,
    combine_and_conditions,
)


class SubqueryDraft:
    """
    Data structure used to progressively build a subquery.
    """

    def __init__(self, data_source: Entity) -> None:
        self.__data_source = data_source
        self.__selected_expressions: Set[SelectedExpression] = set()

    def add_select_expression(self, expression: SelectedExpression) -> None:
        self.__selected_expressions.add(expression)

    def build_query(self) -> ProcessableQuery[Entity]:
        return LogicalQuery(
            {},
            from_clause=self.__data_source,
            selected_columns=list(
                sorted(self.__selected_expressions, key=lambda selected: selected.name)
            ),
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
    expression: Expression, subqueries: Mapping[str, SubqueryDraft]
) -> Expression:
    subexpressions = expression.accept(BranchCutter()).cut_branch()
    for entity_alias, branches in subexpressions.cut_branches.items():
        for branch in branches:
            subqueries[entity_alias].add_select_expression(
                SelectedExpression(name=branch.alias, expression=branch)
            )

    return subexpressions.main_expression


def generate_subqueries(query: CompositeQuery[Entity]) -> None:
    """
    Generates correct subqueries for each of the entities referenced in
    a join query.

    At this stage it pushes down only the columns referenced in the
    external query so that all the referenced columns are present in
    the select clause of the subqueries. We need to push down complex
    expressions to be able to properly process the subqueries.

    Columns in the select clause of the subqueries are referenced
    by providing them a mangled alias that is referenced in the external
    query.

    ```
    SELECT e.a, g.b FROM Events e INNER JOIN Groups g ON ...
    ```

    becomes

    ```
    SELECT e._snuba_a, g._snuba_b
    FROM (
        SELECT a as _snuba_a
        FROM events
    ) e INNER JOIN (
        SELECT b as _snuba_b
        FROM groups
    ) g ON ....
    ```
    """

    from_clause = query.get_from_clause()
    if isinstance(from_clause, CompositeQuery):
        generate_subqueries(from_clause)
        return
    elif isinstance(from_clause, ProcessableQuery):
        return

    # Now this has to be a join, so we can work with it.
    subqueries = from_clause.accept(SubqueriesInitializer())

    # def transform_expression(exp: Expression) -> Expression:
    #    if isinstance(exp, Column):
    #        table_alias = exp.table_name
    #        # All columns in a joined query need to be qualified. We do
    #        # not guess the right subquery for unqualified columns as of
    #        # now.
    #        assert table_alias is not None, f"Invalid query, unqualified column {exp}"
    #        subqueries[table_alias].add_select_expression(
    #            SelectedExpression(
    #                aliasify_column(exp.column_name),
    #                Column(aliasify_column(exp.column_name), None, exp.column_name),
    #            )
    #        )
    #        return Column(None, exp.table_name, aliasify_column(exp.column_name))
    #
    #    return exp
    # query.transform_expressions(transform_expression)
    query.set_ast_selected_columns(
        [
            SelectedExpression(
                name=s.name, expression=_process_root(s.expression, subqueries)
            )
            for s in query.get_selected_columns_from_ast()
        ]
    )

    array_join = query.get_arrayjoin_from_ast()
    if array_join is not None:
        query.set_arrayjoin(_process_root(array_join, subqueries))

    ast_condition = query.get_condition_from_ast()
    if ast_condition is not None:
        query.set_ast_condition(
            combine_and_conditions(
                [
                    _process_root(c, subqueries)
                    for c in get_first_level_and_conditions(ast_condition)
                ]
            )
        )

    query.set_ast_groupby(
        [_process_root(e, subqueries) for e in query.get_groupby_from_ast()]
    )

    having = query.get_condition_from_ast()
    if having is not None:
        query.set_ast_having(
            combine_and_conditions(
                [
                    _process_root(c, subqueries)
                    for c in get_first_level_and_conditions(having)
                ]
            )
        )

    query.set_ast_orderby(
        [
            replace(orderby, expression=_process_root(orderby.expression, subqueries))
            for orderby in query.get_orderby_from_ast()
        ]
    )

    limitby = query.get_limitby()
    if limitby is not None:
        query.set_limitby(
            replace(limitby, expression=_process_root(limitby.expression, subqueries))
        )

    query.set_from_clause(SubqueriesReplacer(subqueries).visit_join_clause(from_clause))
