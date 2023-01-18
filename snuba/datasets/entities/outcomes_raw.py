from typing import Sequence

from snuba.clickhouse.columns import ColumnSet, DateTime
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
)
from snuba.datasets.entity import Entity
from snuba.datasets.plans.storage_plan_builder import StorageQueryPlanBuilder
from snuba.datasets.storage import EntityStorageConnection
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.processors.logical.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.logical.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.logical.quota_processor import ResourceQuotaProcessor
from snuba.query.processors.logical.timeseries_processor import TimeSeriesProcessor
from snuba.query.validation.validators import EntityRequiredColumnValidator


class OutcomesRawEntity(Entity):
    def __init__(self) -> None:
        storage = get_storage(StorageKey.OUTCOMES_RAW)
        read_columns = storage.get_schema().get_columns()
        time_columns = ColumnSet([("time", DateTime())])
        storages = [EntityStorageConnection(storage, TranslationMappers())]
        super().__init__(
            storages=storages,
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=StorageQueryPlanBuilder(
                    storages=storages,
                    selector=DefaultQueryStorageSelector(),
                ),
            ),
            abstract_column_set=read_columns + time_columns,
            join_relationships={},
            validators=[EntityRequiredColumnValidator(["org_id"])],
            required_time_column="timestamp",
            subscription_processors=None,
            subscription_validators=None,
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesProcessor({"time": "timestamp"}, ("timestamp",)),
            ReferrerRateLimiterProcessor(),
            OrganizationRateLimiterProcessor(org_column="org_id"),
            ProjectReferrerRateLimiter("project_id"),
            ResourceQuotaProcessor("project_id"),
        ]
