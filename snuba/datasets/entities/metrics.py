from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from typing import Mapping, Sequence

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entity import Entity
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.extensions import QueryExtension
from snuba.query.processors import QueryProcessor


class MetricsEntity(Entity):
    def __init__(self) -> None:
        writable_storage = get_writable_storage(StorageKey.METRICS_BUCKETS)

        super().__init__(
            # TODO: Add the readable storages
            storages=[writable_storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(writable_storage)
            ),
            abstract_column_set=ColumnSet([]),
            join_relationships={},
            writable_storage=writable_storage,
            required_filter_columns=[],
            required_time_column=None,
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return []
