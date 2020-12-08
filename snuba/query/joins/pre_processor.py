from __future__ import annotations

from typing import Mapping, MutableMapping, NamedTuple, Set

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinNode,
    JoinVisitor,
    entity_from_node,
)
from snuba.query.data_source.simple import Entity


class QualifiedCol(NamedTuple):
    entity: EntityKey
    column: str


# An adjacency set representation of a graph that represents
# semantic equivalence between columns across entities.
# Each node is a QualifiedCol instance, which represents entity
# and column.
# Each edge represent an equivalence between two nodes.
EquivalenceGraph = MutableMapping[QualifiedCol, Set[QualifiedCol]]


class EquivalenceExtractor(JoinVisitor[EquivalenceGraph, Entity]):
    """
    Visits a JoinClause and extracts the relevant the graph of all the
    semantic equivalences between columns across entities involved in
    the Join.

    Equivalences are taken from two source:
    - join ON clause (the join keys). Each key represents the equivalence
      of two columns.
    - equivalences declared in the entity that are not part of the join.
      An example if transaction_name on transactions and spans.

    An EquivalenceGraph is produced.
    """

    def __init__(self, entities_in_join: Set[EntityKey]) -> None:
        # We initialize the visitor with the list of entities present
        # in the join to filter the graph just because extracting this
        # list inside the visitor before we start processing would
        # be quite cumbersome.
        self.__entities = entities_in_join

    def __add_relationship(
        self,
        graph: EquivalenceGraph,
        lhs_entity: EntityKey,
        lhs_column: str,
        rhs_entity: EntityKey,
        rhs_column: str,
    ) -> None:
        """
        Add an equivalence relationship (in both senses as the graph is
        not directed) to the graph.
        """
        left = QualifiedCol(lhs_entity, lhs_column)
        right = QualifiedCol(rhs_entity, rhs_column)
        graph.setdefault(left, set()).add(right)
        graph.setdefault(right, set()).add(left)

    def visit_individual_node(self, node: IndividualNode[Entity]) -> EquivalenceGraph:
        ret: EquivalenceGraph = {}
        entity = get_entity(entity_from_node(node))

        for relationship in entity.get_all_join_relationships().values():
            if relationship.rhs_entity in self.__entities:
                for equivalence in relationship.equivalences:
                    self.__add_relationship(
                        ret,
                        entity_from_node(node),
                        equivalence.left_col,
                        relationship.rhs_entity,
                        equivalence.right_col,
                    )
        return ret

    def visit_join_clause(self, node: JoinClause[Entity]) -> EquivalenceGraph:
        ret: EquivalenceGraph = {}
        mapping = node.get_alias_node_map()
        for condition in node.keys:
            self.__add_relationship(
                ret,
                entity_from_node(mapping[condition.left.table_alias]),
                condition.left.column,
                entity_from_node(mapping[condition.right.table_alias]),
                condition.right.column,
            )

        def merge_into_graph(node: JoinNode[Entity]) -> None:
            for col, equivalences in node.accept(self).items():
                ret[col] = ret.get(col, set()) | equivalences

        merge_into_graph(node.left_node)
        merge_into_graph(node.right_node)
        return ret


def get_equivalent_columns(
    join: JoinClause[Entity],
) -> Mapping[QualifiedCol, Set[QualifiedCol]]:
    """
    Given a Join, it returns the set of all the semantically equivalent
    columns across the entities involved in the join.

    This is obtained by generating, through EquivalenceExtractor, the
    graph of all equivalences.
    We then have the sets of semantically equivalent columns by
    generating the list of connected components in the equivalence graph

    Each node in a connected component of the equivalence graph is by
    definition, semantically equivalent to all the nodes of the same
    connected component (directly if there is an edge between two columns
    or transitively).

    The connected components are returned as a Mapping of nodes to their
    connected component (which is a set of nodes).
    """

    def traverse_graph(
        node: QualifiedCol, visited_nodes: Set[QualifiedCol]
    ) -> Set[QualifiedCol]:
        """
        Traverse the whole connected component in with a depth first
        algorithm starting from the node provided.
        """
        if node in visited_nodes:
            return visited_nodes
        visited_nodes.add(node)
        for n in adjacency_sets.get(node, set()):
            visited_nodes = traverse_graph(n, visited_nodes)
        return visited_nodes

    entities_in_join = {
        entity_from_node(node) for node in join.get_alias_node_map().values()
    }
    adjacency_sets = join.accept(EquivalenceExtractor(entities_in_join))
    connected_components: MutableMapping[QualifiedCol, Set[QualifiedCol]] = {}

    for node in adjacency_sets:
        if node not in connected_components:
            component = traverse_graph(node, set())
            for node in component:
                connected_components[node] = component

    return connected_components
