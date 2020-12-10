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
from snuba.query.expressions import Column, Expression, SubscriptableReference
from snuba.query.logical import Query as LogicalQuery


class SubqueryDraft:
    def __init__(self, data_source: Entity) -> None:
        self.__data_source = data_source
        self.__selected_expressions: Set[SelectedExpression] = set()

    def add_select_expression(self, expression: SelectedExpression) -> None:
        self.__selected_expressions.add(expression)

    def build_query(self) -> ProcessableQuery[Entity]:
        return LogicalQuery(
            {},
            from_clause=self.__data_source,
            selected_columns=list(self.__selected_expressions),
        )


def aliasify_column(col_name: str) -> str:
    return f"_snuba_{col_name}"


class SubqueriesInitializer(JoinVisitor[Mapping[str, SubqueryDraft], Entity]):
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
    def __init__(self, subqueries: Mapping[str, SubqueryDraft]) -> None:
        self.__subqueries = subqueries

    def visit_individual_node(
        self, node: IndividualNode[Entity]
    ) -> IndividualNode[Entity]:
        return IndividualNode(node.alias, self.__subqueries[node.alias].build_query())

    def visit_join_clause(self, node: JoinClause[Entity]) -> JoinClause[Entity]:
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
            assert table_alias is not None, f"Invalid query, unqualified column {exp}"
            subqueries[table_alias].add_select_expression(
                SelectedExpression(
                    aliasify_column(exp.column_name),
                    Column(aliasify_column(exp.column_name), None, exp.column_name),
                )
            )
            return Column(None, exp.table_name, aliasify_column(exp.column_name))

        if isinstance(exp, SubscriptableReference):
            alias = exp.alias or aliasify_column(
                f"{exp.column.column_name}[{exp.key.value}]"
            )
            table_alias = exp.column.table_name
            assert (
                table_alias is not None
            ), f"Invalid query, unqualified subscript {exp}"
            subqueries[table_alias].add_select_expression(
                SelectedExpression(
                    alias, SubscriptableReference(alias, exp.column, exp.key)
                )
            )
            return Column(None, exp.column.table_name, alias)
        return exp

    query.transform_expressions(transform_expression)

    query.set_from_clause(SubqueriesReplacer(subqueries).visit_join_clause(from_clause))
