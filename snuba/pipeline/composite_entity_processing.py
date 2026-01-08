from __future__ import annotations

from typing import Mapping, Union

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.plans.entity_processing import run_entity_processing_executor
from snuba.pipeline.composite_storage_processing import check_sub_query_storage_sets
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity, Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.joins.equivalence_adder import add_equivalent_conditions
from snuba.query.joins.metrics_subquery_generator import generate_metrics_subqueries
from snuba.query.joins.subquery_generator import generate_subqueries
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import QuerySettings


def translate_composite_query(
    query: CompositeQuery[Entity], query_settings: QuerySettings
) -> CompositeQuery[Table]:
    """
    Converts a logical composite query to a physical composite query.
    """
    _pre_entity_query_processing(query)
    physical_query = _translate_logical_composite_query(query, query_settings)
    return physical_query


def _pre_entity_query_processing(query: CompositeQuery[Entity]) -> None:
    """
    Depending on the type of query, this function will execute the necessary
    entity query processings functions to prepare the query for translation.
    """
    from_clause = query.get_from_clause()
    is_gen_metrics_join_query = True
    if isinstance(from_clause, JoinClause):
        nodes = from_clause.get_alias_node_map()
        for node in nodes.values():
            if isinstance(node.data_source, Entity):
                if not node.data_source.key.value.startswith(
                    "generic_metrics"
                ) and not node.data_source.key.value.startswith("metrics"):
                    is_gen_metrics_join_query = False
    else:
        is_gen_metrics_join_query = False

    if is_gen_metrics_join_query:
        generate_metrics_subqueries(query)
    else:
        add_equivalent_conditions(query)
        generate_subqueries(query)


def _translate_logical_composite_query(
    query: CompositeQuery[Entity], settings: QuerySettings
) -> CompositeQuery[Table]:
    """
    Given a logical composite query, this function traverses each sub-query node,
    executes all entity processors associated with each sub-query node,
    and builds a physical composite query.
    """
    translated_source = CompositeDataSourceTransformer(settings).visit(query.get_from_clause())

    return CompositeQuery(
        from_clause=translated_source,
        selected_columns=query.get_selected_columns(),
        array_join=query.get_arrayjoin(),
        condition=query.get_condition(),
        groupby=query.get_groupby(),
        having=query.get_having(),
        order_by=query.get_orderby(),
        limitby=query.get_limitby(),
        limit=query.get_limit(),
        offset=query.get_offset(),
        totals=query.has_totals(),
        granularity=query.get_granularity(),
    )


class CompositeDataSourceTransformer(
    DataSourceVisitor[
        Union[
            ProcessableQuery[Table],
            CompositeQuery[Table],
            JoinClause[Table],
        ],
        Entity,
    ]
):
    """
    A visitor class responsible for traversing through each sub-query node, applies
    entity processors to each node, and returning physical query.
    """

    def __init__(self, query_settings: QuerySettings) -> None:
        self.__settings = query_settings

    def _visit_simple_source(self, data_source: Entity) -> ClickhouseQuery:
        # We are never supposed to get here.
        raise ValueError("Cannot translate a simple Entity")

    def _visit_join(
        self, data_source: JoinClause[Entity]
    ) -> Union[ProcessableQuery[Table], CompositeQuery[Table], JoinClause[Table]]:
        alias_to_query_mappings = data_source.accept(JoinQueryVisitor(self.__settings))
        check_sub_query_storage_sets(alias_to_query_mappings)

        join_visitor = JoinDataSourceTransformer(self.__settings, alias_to_query_mappings)
        join_query = data_source.accept(join_visitor)
        assert isinstance(join_query, JoinClause)
        return join_query

    def _visit_simple_query(
        self, data_source: ProcessableQuery[Entity]
    ) -> Union[ProcessableQuery[Table], CompositeQuery[Table], JoinClause[Table]]:
        assert isinstance(
            data_source, LogicalQuery
        ), f"Only subqueries are allowed at query planning stage. {type(data_source)} found."
        physical_query = run_entity_processing_executor(data_source, self.__settings)
        return physical_query

    def _visit_composite_query(
        self, data_source: CompositeQuery[Entity]
    ) -> Union[ProcessableQuery[Table], CompositeQuery[Table], JoinClause[Table]]:
        return _translate_logical_composite_query(data_source, self.__settings)


class JoinQueryVisitor(JoinVisitor[Mapping[str, ClickhouseQuery], Entity]):
    """
    A visitor class responsible for helping traverse a join query, runs entity processors,
    and builds an alias to physical query mapping.
    """

    def __init__(self, settings: QuerySettings) -> None:
        self.__settings = settings

    def visit_individual_node(self, node: IndividualNode[Entity]) -> Mapping[str, ClickhouseQuery]:
        assert isinstance(
            node.data_source, LogicalQuery
        ), "Invalid composite query. All nodes must be subqueries."
        physical_query = run_entity_processing_executor(node.data_source, self.__settings)
        return {node.alias: physical_query}

    def visit_join_clause(self, node: JoinClause[Entity]) -> Mapping[str, ClickhouseQuery]:
        return {
            **node.left_node.accept(self),
            **node.right_node.accept(self),
        }


class JoinDataSourceTransformer(
    JoinVisitor[Union[JoinClause[Table], IndividualNode[Table]], Entity]
):
    """
    A visitor class responsible for producing a join data source.
    """

    def __init__(
        self,
        settings: QuerySettings,
        alias_to_query_mappings: Mapping[str, ClickhouseQuery],
    ) -> None:
        self.__settings = settings
        self.alias_to_query_mappings = alias_to_query_mappings

    def visit_individual_node(
        self, node: IndividualNode[Entity]
    ) -> Union[JoinClause[Table], IndividualNode[Table]]:
        assert isinstance(
            node.data_source, ProcessableQuery
        ), "Invalid composite query. All nodes must be subqueries."

        query = self.alias_to_query_mappings[node.alias]
        return IndividualNode(alias=node.alias, data_source=query)

    def visit_join_clause(
        self, node: JoinClause[Entity]
    ) -> Union[JoinClause[Table], IndividualNode[Table]]:
        left_node = node.left_node.accept(self)
        right_node = self.visit_individual_node(node.right_node)

        assert isinstance(right_node, IndividualNode)
        return JoinClause(
            left_node=left_node,
            right_node=right_node,
            keys=node.keys,
            join_type=node.join_type,
            join_modifier=node.join_modifier,
        )
