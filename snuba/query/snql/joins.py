from __future__ import annotations
from typing import MutableMapping, NamedTuple, Optional, Sequence, Union

from snuba.datasets.entities import EntityKey
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinRelationship,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.parser.exceptions import ParsingException


class RelationshipTuple(NamedTuple):
    lhs: IndividualNode[QueryEntity]
    relationship: str
    rhs: IndividualNode[QueryEntity]
    data: JoinRelationship


class Node:
    """
    This class is a linked list of entities to join together. Each Node holds its own IndividualNode
    that ultimately is added to a JoinClause. If the Node is not the root of the linked list, it
    will also contain the join conditions that describe which LHS entity the Node should be joined
    and the specifics of that join.

    As new joins are added, they are inserted into the linked list in the correct order so the join
    clause will be nested correctly.

    """

    def __init__(
        self,
        entity_data: IndividualNode[QueryEntity],
        relationship: Optional[JoinRelationship] = None,
    ) -> None:
        self.entity_data = entity_data
        self.relationship = relationship
        self.child: Optional[Node] = None
        self.join_conditions: Sequence[JoinCondition] = []

    @property
    def entity(self) -> EntityKey:
        assert isinstance(self.entity_data.data_source, QueryEntity)
        return self.entity_data.data_source.key

    def push_child(self, node: Node) -> None:
        # This happens here to ensure we use the correct alias in the columns.
        self.build_join_conditions(node)
        if not self.child:
            self.child = node
            return

        old_child = self.child
        self.child = node
        if node.child is None:
            node.child = old_child
            return

        # iterate down list and push old_child to end
        head = node.child
        while head.child is not None:
            head = head.child

        head.child = old_child

    def has_child(self, entity: EntityKey) -> bool:
        if self.entity == entity:
            return True

        if self.child:
            return self.child.has_child(entity)

        return False

    def build_join_conditions(self, rhs: Node) -> None:
        if rhs.relationship is None:
            return

        join_conditions = []
        for lhs_column, rhs_column in rhs.relationship.columns:
            join_conditions.append(
                JoinCondition(
                    left=JoinConditionExpression(self.entity_data.alias, lhs_column),
                    right=JoinConditionExpression(rhs.entity_data.alias, rhs_column),
                )
            )

        rhs.join_conditions = join_conditions


def build_list(relationships: Sequence[RelationshipTuple]) -> Node:
    roots: MutableMapping[EntityKey, Node] = {}
    children: MutableMapping[EntityKey, Node] = {}

    def update_children(child: Optional[Node]) -> None:
        while child is not None:
            children[child.entity] = child
            child = child.child

    for rel in relationships:
        lhs = Node(rel.lhs)
        rhs = Node(rel.rhs, rel.data)
        orphan = roots.get(rhs.entity)
        if orphan:
            if not orphan.has_child(lhs.entity):
                # The orphan is a child of this join. Combine them.
                if orphan.child:
                    rhs.push_child(orphan.child)
                del roots[orphan.entity]

        if lhs.entity in roots:
            roots[lhs.entity].push_child(rhs)
            update_children(rhs)
        else:
            if lhs.entity in children:
                children[lhs.entity].push_child(rhs)
                update_children(rhs)
            else:
                lhs.push_child(rhs)
                roots[lhs.entity] = lhs
                update_children(rhs)

    if len(roots) > 1:
        raise ParsingException("invalid join: join is disconnected")
    if len(roots) < 1:
        raise ParsingException("invalid join: join is cyclical")

    key = list(roots.keys())[0]
    return roots[key]


def build_join_clause_loop(
    node_list: Node,
    lhs: Optional[Union[IndividualNode[QueryEntity], JoinClause[QueryEntity]]],
) -> Union[IndividualNode[QueryEntity], JoinClause[QueryEntity]]:
    rhs = node_list.entity_data
    if lhs is None:
        lhs = rhs
    else:
        assert node_list.relationship is not None  # mypy
        lhs = JoinClause(
            left_node=lhs,
            right_node=rhs,
            keys=node_list.join_conditions,
            join_type=node_list.relationship.join_type,
            join_modifier=node_list.relationship.join_modifier,
        )

    if node_list.child is None:
        return lhs

    return build_join_clause_loop(node_list.child, lhs)


def build_join_clause(
    relationships: Sequence[RelationshipTuple],
) -> JoinClause[QueryEntity]:
    node_list = build_list(relationships)
    clause = build_join_clause_loop(node_list, None)
    assert isinstance(clause, JoinClause)  # mypy
    return clause
