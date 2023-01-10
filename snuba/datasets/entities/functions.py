from typing import Sequence

from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.storage_selectors.selector import (
    ReadableQueryStorageSelector,
)
from snuba.datasets.entity import Entity
from snuba.datasets.plans.storage_plan_builder import StorageQueryPlanBuilder
from snuba.datasets.storage import StorageAndMappers
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.processors.logical.object_id_rate_limiter import (
    ProjectRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.logical.quota_processor import ResourceQuotaProcessor
from snuba.query.validation.validators import EntityRequiredColumnValidator


class FunctionsEntity(Entity):
    def __init__(self) -> None:
        readable_storage = get_storage(StorageKey.FUNCTIONS)
        writable_storage = get_writable_storage(StorageKey.FUNCTIONS_RAW)
        storages = [
            StorageAndMappers(readable_storage, TranslationMappers()),
            StorageAndMappers(writable_storage, TranslationMappers()),
        ]
        schema = readable_storage.get_schema()

        super().__init__(
            storages=storages,
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=StorageQueryPlanBuilder(
                    storages=storages,
                    selector=ReadableQueryStorageSelector(),
                )
            ),
            abstract_column_set=schema.get_columns(),
            join_relationships={},
            writable_storage=writable_storage,
            validators=[
                EntityRequiredColumnValidator(["project_id"]),
            ],
            required_time_column="timestamp",
            subscription_processors=None,
            subscription_validators=None,
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return [
            ReferrerRateLimiterProcessor(),
            ProjectReferrerRateLimiter("project_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
            ResourceQuotaProcessor("project_id"),
        ]
