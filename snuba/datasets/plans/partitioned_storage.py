from typing import Optional, Sequence

import sentry_sdk

from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
)
from snuba.datasets.plans.single_storage import (
    SimpleQueryPlanExecutionStrategy,
    get_query_data_source,
)
from snuba.datasets.plans.translator.query import QueryTranslator
from snuba.datasets.storage import ReadableStorage, StoragePartitionSelector
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.processors.physical.conditions_enforcer import (
    MandatoryConditionEnforcer,
)
from snuba.query.processors.physical.mandatory_condition_applier import (
    MandatoryConditionApplier,
)
from snuba.query.query_settings import QuerySettings
from snuba.util import with_span


class PartitionedStorageQueryPlanBuilder(ClickhouseQueryPlanBuilder):
    """
    Builds the Clickhouse Query Execution Plan for a dataset that is
    partitioned.
    """

    def __init__(
        self,
        storage: ReadableStorage,
        storage_partition_selector: StoragePartitionSelector,
        mappers: Optional[TranslationMappers] = None,
        post_processors: Optional[Sequence[ClickhouseQueryProcessor]] = None,
    ) -> None:
        self.__storage = storage
        self.__storage_partition_selector = storage_partition_selector
        self.__mappers = mappers if mappers is not None else TranslationMappers()
        self.__post_processors = post_processors or []

    @with_span()
    def build_and_rank_plans(
        self, query: LogicalQuery, settings: QuerySettings
    ) -> Sequence[ClickhouseQueryPlan]:
        with sentry_sdk.start_span(
            op="build_plan.partitioned_storage", description="select_storage"
        ):
            query_partition_selection = (
                self.__storage_partition_selector.select_storage(query, settings)
            )

        with sentry_sdk.start_span(
            op="build_plan.partitioned_storage", description="translate"
        ):
            # The QueryTranslator class should be instantiated once for each call to build_plan,
            # to avoid cache conflicts.
            clickhouse_query = QueryTranslator(self.__mappers).translate(query)

        with sentry_sdk.start_span(
            op="build_plan.partitioned_storage", description="set_from_clause"
        ):
            clickhouse_query.set_from_clause(
                get_query_data_source(
                    self.__storage.get_schema().get_data_source(),
                    final=query.get_final(),
                    sampling_rate=query.get_sample(),
                )
            )

        db_query_processors = [
            *self.__storage.get_query_processors(),
            *self.__post_processors,
            MandatoryConditionApplier(),
            MandatoryConditionEnforcer(
                self.__storage.get_mandatory_condition_checkers()
            ),
        ]

        return [
            ClickhouseQueryPlan(
                query=clickhouse_query,
                plan_query_processors=[],
                db_query_processors=db_query_processors,
                storage_set_key=self.__storage.get_storage_set_key(),
                execution_strategy=SimpleQueryPlanExecutionStrategy(
                    cluster=query_partition_selection.cluster,
                    db_query_processors=db_query_processors,
                    splitters=self.__storage.get_query_splitters(),
                ),
            )
        ]
