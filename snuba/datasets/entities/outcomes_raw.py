from datetime import timedelta
from typing import Mapping, Sequence

from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.timeseries_extension import TimeSeriesExtension


class OutcomesRawEntity(Entity):
    def __init__(self) -> None:
        storage = get_storage(StorageKey.OUTCOMES_RAW)

        super().__init__(
            storages=[storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            ),
            abstract_column_set=storage.get_schema().get_columns(),
            join_relationships={},
            writable_storage=None,
            required_filter_columns=["org_id"],
            required_time_column="timestamp",
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=7),
                timestamp_column="timestamp",
            ),
            "organization": OrganizationExtension(),
        }

    def get_prewhere_keys(self) -> Sequence[str]:
        return ["project_id", "org_id"]

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesProcessor({"time": "timestamp"}, ("timestamp",)),
        ]
