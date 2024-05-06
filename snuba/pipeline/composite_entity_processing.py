from __future__ import annotations

from typing import Mapping, Union, cast

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.plans.entity_processing import run_entity_processing_executor
from snuba.datasets.plans.storage_processing import get_query_data_source
from snuba.datasets.plans.translator.query import identity_translate
from snuba.datasets.storages.factory import get_storage
from snuba.pipeline.composite_storage_processing import check_sub_query_storage_sets
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity, LogicalDataSource, Storage, Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.exceptions import InvalidQueryException
from snuba.query.joins.equivalence_adder import add_equivalent_conditions
from snuba.query.joins.subquery_generator import generate_subqueries
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import QuerySettings


def translate_composite_query(
    query: CompositeQuery[Entity], query_settings: QuerySettings
) -> CompositeQuery[Table]:
    """
    Converts a logical composite query to a physical composite query.
    """
    add_equivalent_conditions(query)
    # TODO: unnecessary cast
    generate_subqueries(cast(CompositeQuery[LogicalDataSource], query))
    physical_query = _translate_logical_composite_query(query, query_settings)
    return physical_query


def try_translate_storage_query(
    query: CompositeQuery[LogicalDataSource] | LogicalQuery,
) -> CompositeQuery[Table] | ClickhouseQuery | None:
    finder = _LogicalDataSourceFinder()
    finder.visit(query)
    if finder.is_storage_query:
        if not finder.has_join:
            return _translate_storage_query(query)
        else:
            raise InvalidQueryException("Joins not supporterd for storage queries")
    elif finder.is_mixed_data_source_query:
        raise InvalidQueryException(
            "Queries on storages and entities are not supported",
            data_sources=[str(d) for d in finder.simple_data_sources],
        )
    else:
        # this is not a storage query, pass this on to the rest of the pipeline
        return None


def _translate_logical_composite_query(
    query: CompositeQuery[Entity], settings: QuerySettings
) -> CompositeQuery[Table]:
    """
    Given a logical composite query, this function traverses each sub-query node,
    executes all entity processors associated with each sub-query node,
    and builds a physical composite query.
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


def _translate_storage_query(
    query: CompositeQuery[LogicalDataSource] | LogicalQuery,
) -> CompositeQuery[Table] | ClickhouseQuery:
    # this function assumes there are no joins in storage queries
    from_clause = query.get_from_clause()
    if isinstance(from_clause, Storage) and not isinstance(query, CompositeQuery):
        res = identity_translate(query)
        storage = get_storage(from_clause.key)
        res.set_from_clause(
            get_query_data_source(
                storage.get_schema().get_data_source(),
                allocation_policies=storage.get_allocation_policies(),
                final=query.get_final(),
                sampling_rate=query.get_sample(),
                storage_key=storage.get_storage_key(),
            )
        )
        return res
    else:
        assert isinstance(from_clause, (LogicalQuery, CompositeQuery))
        return CompositeQuery(
            from_clause=_translate_storage_query(from_clause),
            selected_columns=query.get_selected_columns(),
            array_join=query.get_arrayjoin(),
            condition=query.get_condition(),
            groupby=query.get_groupby(),
            having=query.get_having(),
            order_by=query.get_orderby(),
            limitby=query.get_limitby(),
            offset=query.get_offset(),
            totals=query.has_totals(),
            granularity=query.get_granularity(),
        )


class _LogicalDataSourceFinder(
    DataSourceVisitor[
        None,
        LogicalDataSource,
    ],
    JoinVisitor[None, LogicalDataSource],
):
    """Traverses a composite query to understand:

    * what simple data sources exist in the query
    * does the query contain a join

    This is because:
        * Joins are not supported on storage queries (at time of writing (05/05/24)
        * Composite queries where storages and entities are mixed are not supported

    Usage:
        >>> finder = _LogicalDataSourceFinder()
        >>> finder.visit(query)
        >>> assert len(finder.simple_data_sources) == 1
        >>> assert not finder.has_join
    """

    @property
    def is_storage_query(self) -> bool:
        return all((isinstance(d, Storage) for d in self.simple_data_sources))

    @property
    def is_mixed_data_source_query(self) -> bool:
        return any((isinstance(d, Storage) for d in self.simple_data_sources)) and any(
            (isinstance(d, Entity) for d in self.simple_data_sources)
        )

    def __init__(self) -> None:
        self.has_join = False
        self.simple_data_sources: list[LogicalDataSource] = []

    def _visit_simple_source(self, data_source: LogicalDataSource) -> None:
        self.simple_data_sources.append(data_source)

    def _visit_join(self, data_source: JoinClause[LogicalDataSource]) -> None:
        self.has_join = True
        return self.visit_join_clause(data_source)

    def _visit_simple_query(
        self, data_source: ProcessableQuery[LogicalDataSource]
    ) -> None:
        self.visit(data_source.get_from_clause())

    def _visit_composite_query(
        self, data_source: CompositeQuery[LogicalDataSource]
    ) -> None:
        self.visit(data_source.get_from_clause())

    def visit_individual_node(self, node: IndividualNode[LogicalDataSource]) -> None:
        self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[LogicalDataSource]) -> None:
        node.left_node.accept(self)
        node.right_node.accept(self)


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

        join_visitor = JoinDataSourceTransformer(
            self.__settings, alias_to_query_mappings
        )
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

    def visit_individual_node(
        self, node: IndividualNode[Entity]
    ) -> Mapping[str, ClickhouseQuery]:
        assert isinstance(
            node.data_source, LogicalQuery
        ), "Invalid composite query. All nodes must be subqueries."
        physical_query = run_entity_processing_executor(
            node.data_source, self.__settings
        )
        return {node.alias: physical_query}

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
