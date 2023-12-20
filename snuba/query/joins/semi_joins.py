from typing import Set

from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinModifier,
    JoinNode,
    JoinType,
    JoinVisitor,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column
from snuba.query.processors.physical import CompositeQueryProcessor
from snuba.query.query_settings import QuerySettings


class SemiJoinOptimizer(CompositeQueryProcessor):
    """
    Processes a composite query to find join clause that can be
    executed as SEMI joins, instead of building the entire join
    hashtable.

    A semi join is a join where the query only references columns
    from the left table (it can be used as an INNER join to filter
    rows of the left table without referencing anything from the
    right table.

    For such queries the DB does not need to fetch all the rows
    of the right table that correspond to each row of the left
    table, thus reducing drastically the memory footprint of the
    query.

    So this processor navigates the leaves of the join tree from
    left to right and turn all the join clauses that fit in the
    conditions above into SEMI joins.
    As soon as a join clause cannot be transformed we stop there

    """

    def process_query(
        self, query: CompositeQuery[Table], query_settings: QuerySettings
    ) -> None:
        from_clause = query.get_from_clause()
        if not isinstance(from_clause, JoinClause):
            return

        # Now this has to be a join, so we can work with it.
        query.set_from_clause(
            SemiJoinGenerator(query.get_all_ast_referenced_columns()).visit_join_clause(
                from_clause
            )
        )


class SemiJoinGenerator(JoinVisitor[JoinNode[Table], Table]):
    def __init__(self, referenced_columns: Set[Column]) -> None:
        self.__referenced_columns = referenced_columns

    def visit_individual_node(
        self, node: IndividualNode[Table]
    ) -> IndividualNode[Table]:
        return node

    def visit_join_clause(self, node: JoinClause[Table]) -> JoinClause[Table]:
        # TODO: Skip this optimization if the left side of the query
        # runs aggregations that imply the cardinality of the right
        # side is important.

        join_cols = set()

        for condition in node.keys:
            if condition.left.table_alias == node.right_node.alias:
                join_cols.add(condition.left.column)
            elif condition.right.table_alias == node.right_node.alias:
                join_cols.add(condition.right.column)

        for c in self.__referenced_columns:
            if c.table_name == node.right_node.alias and c.column_name not in join_cols:
                return node

        return JoinClause(
            left_node=node.left_node.accept(self),
            right_node=node.right_node,
            keys=node.keys,
            join_type=node.join_type,
            join_modifier=JoinModifier.ANY
            if node.join_type == JoinType.INNER
            else JoinModifier.SEMI,
        )
