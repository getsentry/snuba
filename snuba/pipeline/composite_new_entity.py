from __future__ import annotations

from typing import Mapping, Union

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clusters.storage_sets import is_valid_storage_set_combination
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity, Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.joins.equivalence_adder import add_equivalent_conditions
from snuba.query.joins.subquery_generator import generate_subqueries
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import QuerySettings


def translate_logical_composite_query(
    query: CompositeQuery[Entity], settings: QuerySettings
) -> CompositeQuery:
    """
    Given a logical composite query, this function traverses each subquery node,
    executes entity processing, and builds a single physical composite query.
    """

    translated_source = CompositeDataSourceTransformer(settings).visit(
        query.get_from_clause()
    )

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


def check_sub_query_storage_sets(
    alias_to_query_mappings: Mapping[str, ClickhouseQuery]
) -> None:
    """
    Receives a mapping with all the valid query for each subquery
    in a join, and checks that queries are grouped in JOINABLE_STORAGE_SETS.
    """
    from snuba.datasets.storages.factory import get_storage
    from snuba.pipeline.utils.storage_finder import StorageKeyFinder

    all_storage_sets = []
    for _, query in alias_to_query_mappings.items():
        storage_key = StorageKeyFinder().visit(query)
        storage = get_storage(storage_key)
        all_storage_sets.append(storage.get_storage_set_key())

    is_valid_storage_set_combination(*all_storage_sets)


class CompositeDataSourceTransformer(
    DataSourceVisitor[
        Union[
            ClickhouseQuery, CompositeQuery, JoinClause[Table], IndividualNode[Table]
        ],
        Entity,
    ]
):
    """
    A visitor class responsible for producing a data source whether it be a ClickhouseQuery,
    CompositeQuery, JoinClause[Table], or IndividualNode[Table]. This visitor traverses through
    each node, applies the entity processing, and builds up a single data source.
    """

    def __init__(self, query_settings: QuerySettings) -> None:
        self.__settings = query_settings

    def _visit_simple_source(self, data_source: Entity) -> ClickhouseQuery:
        # We are never supposed to get here.
        raise ValueError("Cannot translate a simple Entity")

    def _visit_join(
        self, data_source: JoinClause[Entity]
    ) -> Union[JoinClause[Table], IndividualNode[Table]]:
        alias_to_query_mappings = data_source.accept(JoinQueryVisitor(self.__settings))
        check_sub_query_storage_sets(alias_to_query_mappings)

        join_visitor = JoinDataSourceTransformer(
            self.__settings, alias_to_query_mappings
        )
        join_query = data_source.accept(join_visitor)
        return join_query

    def _visit_simple_query(
        self, data_source: ProcessableQuery[Entity]
    ) -> ClickhouseQuery:
        assert isinstance(
            data_source, LogicalQuery
        ), f"Only subqueries are allowed at query planning stage. {type(data_source)} found."
        entity = get_entity(data_source.get_from_clause().key)
        assert isinstance(entity, PluggableEntity)
        entity_processing_executor = entity.get_processing_executor()
        query = entity_processing_executor.execute(data_source, self.__settings)
        return query

    def _visit_composite_query(
        self, data_source: CompositeQuery[Entity]
    ) -> CompositeQuery:
        return translate_logical_composite_query(data_source, self.__settings)


class JoinQueryVisitor(JoinVisitor[Mapping[str, ClickhouseQuery], Entity]):
    """
    A visitor class responsible for helping traverse a join query and builds
    an alias to physical query mapping.
    """

    def __init__(self, settings: QuerySettings) -> None:
        self.__settings = settings

    def visit_individual_node(
        self, node: IndividualNode[Entity]
    ) -> Mapping[str, ClickhouseQuery]:
        assert isinstance(
            node.data_source, LogicalQuery
        ), "Invalid composite query. All nodes must be subqueries."

        entity = get_entity(node.data_source.get_from_clause().key)
        assert isinstance(entity, PluggableEntity)
        entity_processing_executor = entity.get_processing_executor()
        query = entity_processing_executor.execute(node.data_source, self.__settings)

        return {node.alias: query}

    def visit_join_clause(
        self, node: JoinClause[Entity]
    ) -> Mapping[str, ClickhouseQuery]:
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


def composite_query_entity_processing_and_translation(
    query: CompositeQuery[Entity], query_settings: QuerySettings
) -> CompositeQuery:
    add_equivalent_conditions(query)
    generate_subqueries(query)
    physical_query = translate_logical_composite_query(query, query_settings)
    return physical_query
