from __future__ import annotations

from snuba.datasets.slicing import is_storage_set_sliced
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Table
from snuba.query.data_source.visitor import DataSourceVisitor


class StorageKeyJoinFinder(JoinVisitor[StorageKey, Table]):
    """
    looks through the join clause for relevant storage keys. picks an arbitrary one to execute the
    query against, prioritizes sliced storages
    """

    def visit_individual_node(self, node: IndividualNode[Table]) -> StorageKey:
        if isinstance(node.data_source, ProcessableQuery):
            return node.data_source.get_from_clause().storage_key
        else:
            return node.data_source.storage_key

    def visit_join_clause(self, node: JoinClause[Table]) -> StorageKey:
        left_storage_key = node.left_node.accept(self)
        right_storage_key = node.right_node.accept(self)
        if is_storage_set_sliced(get_storage(left_storage_key).get_storage_set_key()):
            return left_storage_key
        return right_storage_key


class StorageKeyFinder(DataSourceVisitor[StorageKey, Table]):
    """
    Given a query, finds the storage_set_key from which to get the cluster
    In the case of a join, it will select an arbitrary storage_set_key that is in
    the from_clause. Storages that are sliced are given higher priority

    """

    def _visit_simple_source(self, data_source: Table) -> StorageKey:
        return data_source.storage_key

    def _visit_join(self, data_source: JoinClause[Table]) -> StorageKey:
        return data_source.accept(StorageKeyJoinFinder())

    def _visit_simple_query(self, data_source: ProcessableQuery[Table]) -> StorageKey:
        return data_source.get_from_clause().storage_key

    def _visit_composite_query(self, data_source: CompositeQuery[Table]) -> StorageKey:
        return self.visit(data_source.get_from_clause())
