# from __future__ import annotations
from snuba.datasets.entities.factory import get_entity

from typing import Mapping, MutableMapping, NamedTuple, Optional, Set

from snuba.datasets.entities import EntityKey


class QualifiedCol(NamedTuple):
    entity: EntityKey
    column: str


# Stores an expanded lazily generated map of all the semantically
# equivalent columns in a dataset. This is used to add implied
# conditions in joins that may not be present in the provided
# query.
# An example is a join between spans and transactions. If the
# query contains a condition on transactions.transaction_name,
# we can automatically add the same condition on the spans
# subquery. That will reduce the size of the subquery resultset.
COLUMN_EQUIVALENCES: Optional[Mapping[QualifiedCol, Set[QualifiedCol]]] = {}


def _compute_entity_equivalences() -> Mapping[QualifiedCol, Set[QualifiedCol]]:
    def add_relationship(
        lhs_entity: EntityKey, lhs_column: str, rhs_entity: EntityKey, rhs_column: str
    ) -> None:
        left = QualifiedCol(lhs_entity, lhs_column)
        right = QualifiedCol(rhs_entity, rhs_column)
        if left not in equivalence_adjacency_set:
            equivalence_adjacency_set[left] = {right}
        else:
            equivalence_adjacency_set[left].add(right)
        if right not in equivalence_adjacency_set:
            equivalence_adjacency_set[right] = {left}
        else:
            equivalence_adjacency_set[right].add(left)

    # Pre-process the graph by collecting all the edges for all the entities.
    equivalence_adjacency_set: MutableMapping[QualifiedCol, Set[QualifiedCol]] = {}
    for key in list(EntityKey):
        entity = get_entity(key)
        for relationship in entity.get_all_join_relationships().values():
            dest_entity = relationship.rhs_entity
            for condition in relationship.keys:
                add_relationship(
                    key, condition.left.column, dest_entity, condition.right.column
                )
            for equivalence in relationship.equivalences:
                add_relationship(
                    key, equivalence.left_col, dest_entity, equivalence.right_col
                )

    # Calculates the connected components of the equivalences graph.
    def traverse_node(
        node: QualifiedCol, visited_nodes: Set[QualifiedCol]
    ) -> Set[QualifiedCol]:
        if node in visited_nodes:
            return visited_nodes
        visited_nodes.add(node)
        for n in equivalence_adjacency_set.get(node, set()):
            visited_nodes = traverse_node(n, visited_nodes)
        return visited_nodes

    connected_components: MutableMapping[QualifiedCol, Set[QualifiedCol]] = {}
    for node in equivalence_adjacency_set:
        if node not in connected_components:
            component = traverse_node(node, set())
            for node in component:
                connected_components[node] = component

    return connected_components


def get_equivalent_columns(
    entity: EntityKey, column: str
) -> Optional[Set[QualifiedCol]]:
    """
    Given a dataset, an entity name and a column name, this returns all
    the semantically equivalent columns in other entities in the same
    dataset.
    """
    global COLUMN_EQUIVALENCES
    if COLUMN_EQUIVALENCES is None:
        COLUMN_EQUIVALENCES = _compute_entity_equivalences()

    return COLUMN_EQUIVALENCES.get(QualifiedCol(entity, column))
