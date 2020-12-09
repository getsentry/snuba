from functools import partial
from typing import Mapping, Optional, Set

from snuba.datasets.entities import EntityKey
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    combine_and_conditions,
    get_first_level_and_conditions,
)
from snuba.query.data_source.join import (
    entity_from_node,
    JoinVisitor,
    IndividualNode,
    JoinClause,
)
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, Expression
from snuba.query.joins.pre_processor import QualifiedCol, get_equivalent_columns


class SubqueryDraft:
    def __init__(self, data_source: Entity) -> None:
        self.__data_source = data_source
        # TODO: Support subscriptable references
        self.__selected_cols: Set[str] = set()

    def add_column(self, col_name: str) -> None:
        self.__selected_cols.add(col_name)

    def build_query(self) -> ProcessableQuery[Entity]:
        raise NotImplementedError


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
            combined[condition.left.table_alias].add_column(condition.left.column)
            combined[condition.right.table_alias].add_column(condition.right.column)
        return combined


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
