from typing import Sequence

from snuba.clickhouse.columns import ColumnSet, DateTime
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.quota_processor import ResourceQuotaProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.validation.validators import EntityRequiredColumnValidator


class OutcomesRawEntity(Entity):
    def __init__(self) -> None:
        storage = get_storage(StorageKey.OUTCOMES_RAW)
        read_columns = storage.get_schema().get_columns()
        time_columns = ColumnSet([("time", DateTime())])
        super().__init__(
            storages=[storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            ),
            abstract_column_set=read_columns + time_columns,
            join_relationships={},
            writable_storage=None,
            validators=[EntityRequiredColumnValidator({"org_id"})],
            required_time_column="timestamp",
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesProcessor({"time": "timestamp"}, ("timestamp",)),
            ReferrerRateLimiterProcessor(),
            OrganizationRateLimiterProcessor(org_column="org_id"),
            ProjectReferrerRateLimiter("project_id"),
            ResourceQuotaProcessor("project_id"),
        ]
