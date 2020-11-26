from __future__ import annotations
from typing import MutableMapping, NamedTuple, Optional, Sequence, Union

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinRelationship,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.parser.exceptions import ParsingException


class EntityTuple(NamedTuple):
    alias: str
    name: str
    sample_rate: Optional[float] = None


class RelationshipTuple(NamedTuple):
    lhs: EntityTuple
    relationship: str
    rhs: EntityTuple
    data: JoinRelationship


class Leaf(object):
    def __init__(
        self,
        entity: EntityKey,
        entity_data: EntityTuple,
        relationship: Optional[JoinRelationship] = None,
    ) -> None:
        self.entity = entity
        self.entity_data = entity_data
        self.relationship = relationship
        self.child: Optional[Leaf] = None
        self.join_conditions: Sequence[JoinCondition] = []

    def push_leaf(self, leaf: Leaf) -> None:
        # This happens here to ensure we use the correct alias in the columns.
        self.build_join_conditions(leaf)
        if not self.child:
            self.child = leaf
            return

        old_child = self.child
        self.child = leaf
        if leaf.child is None:
            leaf.child = old_child
            return

        # iterate down list and push old_child to end
        head = leaf.child
        while head.child is not None:
            head = head.child

        head.child = old_child

    def has_child(self, entity: EntityKey) -> bool:
        if self.entity == entity:
            return True

        if self.child:
            return self.child.has_child(entity)

        return False

    def build_join_conditions(self, rhs: Leaf) -> None:
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


def build_tree(relationships: Sequence[RelationshipTuple]) -> Leaf:
    roots: MutableMapping[EntityKey, Leaf] = {}
    leafs: MutableMapping[EntityKey, Leaf] = {}

    def update_leafs(child: Optional[Leaf]) -> None:
        while child is not None:
            leafs[child.entity] = child
            child = child.child

    for rel in relationships:
        lhs = Leaf(EntityKey(rel.lhs.name), rel.lhs)
        rhs = Leaf(EntityKey(rel.rhs.name), rel.rhs, rel.data)
        orphan = roots.get(rhs.entity)
        if orphan:
            if not orphan.has_child(lhs.entity):
                # The orphan is a child of this join. Combine them.
                if orphan.child:
                    rhs.push_leaf(orphan.child)
                del roots[orphan.entity]

        if lhs.entity in roots:
            roots[lhs.entity].push_leaf(rhs)
            update_leafs(rhs)
        else:
            if lhs.entity in leafs:
                leafs[lhs.entity].push_leaf(rhs)
                update_leafs(rhs)
            else:
                lhs.push_leaf(rhs)
                roots[lhs.entity] = lhs
                update_leafs(rhs)

    if len(roots) > 1:
        raise ParsingException("invalid join: join is disconnected")
    if len(roots) < 1:
        raise ParsingException("invalid join: join is cyclical")

    key = list(roots.keys())[0]
    return roots[key]


def build_join_clause_loop(
    tree: Leaf,
    lhs: Optional[Union[IndividualNode[QueryEntity], JoinClause[QueryEntity]]],
) -> Union[IndividualNode[QueryEntity], JoinClause[QueryEntity]]:
    rhs = IndividualNode(
        tree.entity_data.alias,
        QueryEntity(
            tree.entity,
            get_entity(tree.entity).get_data_model(),
            tree.entity_data.alias,
            tree.entity_data.sample_rate,
        ),
    )
    if lhs is None:
        lhs = rhs
    else:
        assert tree.relationship is not None  # mypy
        lhs = JoinClause(
            left_node=lhs,
            right_node=rhs,
            keys=tree.join_conditions,
            join_type=tree.relationship.join_type,
            join_modifier=tree.relationship.join_modifier,
        )

    if tree.child is None:
        return lhs

    return build_join_clause_loop(tree.child, lhs)


def build_join_clause(
    relationships: Sequence[RelationshipTuple],
) -> JoinClause[QueryEntity]:
    tree = build_tree(relationships)
    clause = build_join_clause_loop(tree, None)
    assert isinstance(clause, JoinClause)  # mypy
    return clause
