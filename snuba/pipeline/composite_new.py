from __future__ import annotations

from typing import Mapping, NamedTuple, Optional, Sequence, Tuple, Union

import sentry_sdk

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clusters.cluster import ClickhouseCluster
from snuba.clusters.storage_sets import StorageSetKey, is_valid_storage_set_combination
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    CompositeQueryPlan,
    QueryPlanExecutionStrategy,
    QueryRunner,
    SubqueryProcessors,
)
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity, Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.joins.equivalence_adder import add_equivalent_conditions
from snuba.query.joins.subquery_generator import generate_subqueries
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.physical import (
    ClickhouseQueryProcessor,
    CompositeQueryProcessor,
)
from snuba.query.query_settings import QuerySettings
from snuba.state import explain_meta
from snuba.web import QueryResult


def translate_logical_composite_query(
    query: CompositeQuery[Entity], settings: QuerySettings
) -> CompositeQuery:
    """
    Produces a composite query plan out of a composite query.

    This is the bulk of the logic of The Composite Planner. It is kept
    in its own function because it needs to be used by the data source
    visitor when planning subqueries (which can be composite as well).
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
    in a join, and checks that queries are in the same storage set.
    """
    from snuba.datasets.storages.factory import get_storage
    from snuba.pipeline.utils.storage_finder import StorageKeyFinder

    all_storage_sets = []
    for alias, query in alias_to_query_mappings.items():
        # get storage set key from each subquery
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
    Plans the DataSource (the from clause) of a Composite query by
    visiting each node as the data source can be composite itself.

    At each step an intermediate data structure containing the
    translated query and the query processors to run in the execution
    strategy is produced.
    These intermediate structures are merged together and produce the
    CompositeQueryPlan.
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
    Produces all ClickhouseQuerys for each subquery in the join.
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
    TODO
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


class JoinDataSourcePlan(NamedTuple):
    """
    Intermediate data structure maintained when visiting and
    transforming a join in the composite query data source.

    We cannot use a Composite plan itself as an intermediate
    structure thus the need of this class.
    This is because the data source type is a bit less
    restrictive in this class and we would have to instantiate
    an execution strategy that is not used.
    """

    translated_source: Union[JoinClause[Table], IndividualNode[Table]]
    processors: Mapping[str, SubqueryProcessors]
    storage_set_key: StorageSetKey


class JoinDataSourcePlanner(JoinVisitor[JoinDataSourcePlan, Entity]):
    """
    Plans a join data source inside a composite query producing a
    JoinDataSourcePlan.
    This finds the simple subqueries, plan them with _plan_simple_query
    and keeps a structure containing all the query processors the
    execution strategy will have to run.

    These processors are stored in a mapping with their table alias.
    """

    def __init__(
        self, settings: QuerySettings, plans: Mapping[str, ClickhouseQueryPlan]
    ) -> None:
        self.__settings = settings
        self.__plans = plans

    def visit_individual_node(self, node: IndividualNode[Entity]) -> JoinDataSourcePlan:
        assert isinstance(
            node.data_source, ProcessableQuery
        ), "Invalid composite query. All nodes must be subqueries."

        sub_query_plan = self.__plans[node.alias]
        return JoinDataSourcePlan(
            translated_source=IndividualNode(
                alias=node.alias, data_source=sub_query_plan.query
            ),
            processors={
                node.alias: SubqueryProcessors(
                    plan_processors=sub_query_plan.plan_query_processors,
                    db_processors=sub_query_plan.db_query_processors,
                )
            },
            storage_set_key=sub_query_plan.storage_set_key,
        )

    def visit_join_clause(self, node: JoinClause[Entity]) -> JoinDataSourcePlan:
        left_node = node.left_node.accept(self)
        right_node = self.visit_individual_node(node.right_node)

        # TODO: Actually return multiple plans for each subquery (one per storage
        # set) and rank them picking a combination that fits in a single storage
        # set or valid storage set combination.
        assert is_valid_storage_set_combination(
            left_node.storage_set_key, right_node.storage_set_key
        ), f"Incompatible storage sets found in plan: {left_node.storage_set_key} {right_node.storage_set_key}"

        # mypy does not know that the method above only produces individual nodes.
        assert isinstance(right_node.translated_source, IndividualNode)
        return JoinDataSourcePlan(
            translated_source=JoinClause(
                left_node=left_node.translated_source,
                right_node=right_node.translated_source,
                keys=node.keys,
                join_type=node.join_type,
                join_modifier=node.join_modifier,
            ),
            processors={**left_node.processors, **right_node.processors},
            storage_set_key=left_node.storage_set_key,
        )


class CompositeDataSourcePlan(NamedTuple):
    """
    Intermediate data structure maintained when visiting and
    transforming the query data source.

    We cannot use a Composite plan itself as an intermediate
    structure thus the need of this class.
    This is because the data source type is a bit less
    restrictive in this class and we would have to instantiate
    an execution strategy that is not used.
    """

    translated_source: Union[ClickhouseQuery, CompositeQuery[Table], JoinClause[Table]]
    storage_set_key: StorageSetKey
    root_processors: Optional[SubqueryProcessors] = None
    aliased_processors: Optional[Mapping[str, SubqueryProcessors]] = None

    @classmethod
    def from_simple_query_plan(
        cls, query_plan: ClickhouseQueryPlan
    ) -> CompositeDataSourcePlan:
        return CompositeDataSourcePlan(
            translated_source=query_plan.query,
            root_processors=SubqueryProcessors(
                plan_processors=query_plan.plan_query_processors,
                db_processors=query_plan.db_query_processors,
            ),
            storage_set_key=query_plan.storage_set_key,
        )

    @classmethod
    def from_composite_subquery(
        cls, query_plan: CompositeQueryPlan
    ) -> CompositeDataSourcePlan:
        return CompositeDataSourcePlan(
            translated_source=query_plan.query,
            root_processors=query_plan.root_processors,
            aliased_processors=query_plan.aliased_processors,
            storage_set_key=query_plan.storage_set_key,
        )

    def get_db_processors(
        self,
    ) -> Tuple[
        Sequence[ClickhouseQueryProcessor],
        Mapping[str, Sequence[ClickhouseQueryProcessor]],
    ]:
        return (
            (
                self.root_processors.db_processors
                if self.root_processors is not None
                else []
            ),
            (
                {
                    alias: subquery.db_processors
                    for alias, subquery in self.aliased_processors.items()
                }
                if self.aliased_processors is not None
                else {}
            ),
        )


class ProcessorsExecutor(DataSourceVisitor[None, Table], JoinVisitor[None, Table]):
    """
    Applies in place the a sequence of query processors to the subqueries
    of a composite query.
    Processors are provided as a sequence of processors to be applied to
    the root subquery or a mapping of sequences (one per alias) to be
    applied to subqueries in a join. In the join case there are multiple
    subqueries with a table alias in the composite query.
    """

    def __init__(
        self,
        root_processors: Sequence[ClickhouseQueryProcessor],
        aliased_processors: Mapping[str, Sequence[ClickhouseQueryProcessor]],
        query_settings: QuerySettings,
    ) -> None:
        self.__root_processors = root_processors
        self.__aliased_processors = aliased_processors
        self.__settings = query_settings

    def __process_simple_query(
        self,
        clickhouse_query: ClickhouseQuery,
        processors: Sequence[ClickhouseQueryProcessor],
    ) -> None:
        for clickhouse_processor in processors:
            with sentry_sdk.start_span(
                description=type(clickhouse_processor).__name__, op="processor"
            ):
                clickhouse_processor.process_query(clickhouse_query, self.__settings)

    def _visit_simple_source(self, data_source: Table) -> None:
        # We are never supposed to get here.
        raise ValueError("Cannot translate a simple Entity")

    def _visit_join(self, data_source: JoinClause[Table]) -> None:
        data_source.accept(self)

    def _visit_simple_query(self, data_source: ProcessableQuery[Table]) -> None:
        assert isinstance(data_source, ClickhouseQuery)
        self.__process_simple_query(data_source, self.__root_processors)

    def _visit_composite_query(self, data_source: CompositeQuery[Table]) -> None:
        self.visit(data_source.get_from_clause())

    def visit_individual_node(self, node: IndividualNode[Table]) -> None:
        assert isinstance(
            node.data_source, ClickhouseQuery
        ), "Invalid join structure. Only subqueries are allowed at this stage."
        self.__process_simple_query(
            node.data_source, self.__aliased_processors[node.alias]
        )

    def visit_join_clause(self, node: JoinClause[Table]) -> None:
        node.left_node.accept(self)
        node.right_node.accept(self)


class CompositeExecutionStrategy(QueryPlanExecutionStrategy[CompositeQuery[Table]]):
    """
    Executes a composite query after applying the db query processors
    to each subquery.
    """

    def __init__(
        self,
        cluster: ClickhouseCluster,
        root_processors: Sequence[ClickhouseQueryProcessor],
        aliased_processors: Mapping[str, Sequence[ClickhouseQueryProcessor]],
        composite_processors: Sequence[CompositeQueryProcessor],
    ) -> None:
        self.__cluster = cluster
        self.__root_processors = root_processors
        self.__aliased_processors = aliased_processors
        # Processors that are applied to the entire composite query
        # by the execution strategy before running the query on the DB
        # and after all subquery processors have been executed
        self.__composite_processors = composite_processors

    def execute(
        self,
        query: CompositeQuery[Table],
        query_settings: QuerySettings,
        runner: QueryRunner,
    ) -> QueryResult:
        ProcessorsExecutor(
            self.__root_processors, self.__aliased_processors, query_settings
        ).visit(query)

        for p in self.__composite_processors:
            if query_settings.get_dry_run():
                with explain_meta.with_query_differ(
                    "composite_storage_processor", type(p).__name__, query
                ):
                    p.process_query(query, query_settings)
            else:
                p.process_query(query, query_settings)

        return runner(
            clickhouse_query=query,
            query_settings=query_settings,
            reader=self.__cluster.get_reader(),
            cluster_name=self.__cluster.get_clickhouse_cluster_name() or "",
        )


# class CompositeExecutionPipeline(QueryExecutionPipeline):
#     """
#     Executes a logical composite query by generating a plan,
#     applying the plan query processors to all subqueries and
#     handing the result to the execution strategy.
#     """

#     def __init__(
#         self,
#         query: CompositeQuery[Entity],
#         query_settings: QuerySettings,
#         runner: QueryRunner,
#     ):
#         self.__query = query
#         self.__settings = query_settings
#         self.__runner = runner

#     def execute(self) -> QueryResult:
#         plan = CompositeQueryPlanner(self.__query, self.__settings).build_best_plan()
#         root_processors, aliased_processors = plan.get_plan_processors()
#         ProcessorsExecutor(
#             root_processors,
#             aliased_processors,
#             self.__settings,
#         ).visit(plan.query)

#         return plan.execution_strategy.execute(
#             plan.query, self.__settings, self.__runner
#         )


class EntityCompositeProcessingExecutor:
    def execute(
        self, query: CompositeQuery[Entity], query_settings: QuerySettings
    ) -> CompositeQuery:
        add_equivalent_conditions(query)
        generate_subqueries(query)
        physical_query = translate_logical_composite_query(query, query_settings)
        return physical_query
