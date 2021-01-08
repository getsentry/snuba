from typing import Set

from snuba.clickhouse.processors import CompositeQueryProcessor
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import JoinClause, JoinModifier
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column
from snuba.request.request_settings import RequestSettings


class SemiJoinOptimizer(CompositeQueryProcessor):
    """
    Turn a join into a SEMI JOIN when possible.
    """

    def process_query(
        self, query: CompositeQuery[Table], request_settings: RequestSettings
    ) -> None:
        from_clause = query.get_from_clause()
        if isinstance(from_clause, CompositeQuery):
            self.process_query(from_clause, request_settings)
            return
        elif isinstance(from_clause, ProcessableQuery):
            return

        # Now this has to be a join, so we can work with it.
        query.set_from_clause(
            self._process_clause(from_clause, query.get_all_ast_referenced_columns())
        )

    def _process_clause(
        self, join_clause: JoinClause[Table], referenced_columns: Set[Column]
    ) -> JoinClause[Table]:
        join_cols = set()
        for condition in join_clause.keys:
            if condition.left.table_alias == join_clause.right_node.alias:
                join_cols.add(condition.left.column)
            elif condition.right.table_alias == join_clause.right_node.alias:
                join_cols.add(condition.right.column)

        for c in referenced_columns:
            if (
                c.table_name == join_clause.right_node.alias
                and c.column_name not in join_cols
            ):
                return join_clause

        return JoinClause(
            left_node=self._process_clause(join_clause.left_node, referenced_columns)
            if isinstance(join_clause.left_node, JoinClause)
            else join_clause.left_node,
            right_node=join_clause.right_node,
            keys=join_clause.keys,
            join_type=join_clause.join_type,
            join_modifier=JoinModifier.SEMI,
        )
