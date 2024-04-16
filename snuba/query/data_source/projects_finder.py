from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity, Storage
from snuba.query.data_source.visitor import DataSourceVisitor


class ProjectsFinder(
    DataSourceVisitor[set[int], Entity | Storage],
    JoinVisitor[set[int], Entity | Storage],
):
    """
    Traverses a query to find project_id conditions
    """

    def _visit_simple_source(self, data_source: Entity | Storage) -> set[int]:
        return set()

    def _visit_join(self, data_source: JoinClause[Entity | Storage]) -> set[int]:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(
        self, data_source: ProcessableQuery[Entity | Storage]
    ) -> set[int]:
        return get_object_ids_in_query_ast(data_source, "project_id") or set()

    def _visit_composite_query(
        self, data_source: CompositeQuery[Entity | Storage]
    ) -> set[int]:
        return self.visit(data_source.get_from_clause())

    def visit_individual_node(self, node: IndividualNode[Entity | Storage]) -> set[int]:
        return self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[Entity | Storage]) -> set[int]:
        left = node.left_node.accept(self)
        right = node.right_node.accept(self)
        return left | right
