from typing import Mapping, Set

from snuba.query import ProcessableQuery, SelectedExpression
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

    def transform_expression(exp: Expression) -> Expression:
        if isinstance(exp, Column):
            table_alias = exp.table_name
            # All columns in a joined query need to be qualified. We do
            # not guess the right subquery for unqualified columns as of
            # now.
            assert table_alias is not None, f"Invalid query, unqualified column {exp}"
            subqueries[table_alias].add_select_expression(
                SelectedExpression(
                    aliasify_column(exp.column_name),
                    Column(aliasify_column(exp.column_name), None, exp.column_name),
                )
            )
            return Column(None, exp.table_name, aliasify_column(exp.column_name))

        return exp

    query.transform_expressions(transform_expression)

    query.set_from_clause(SubqueriesReplacer(subqueries).visit_join_clause(from_clause))
