from typing import Sequence

from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors import QueryProcessor
from snuba.query.processors.object_id_rate_limiter import (
    ProjectRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.quota_processor import ResourceQuotaProcessor
from snuba.query.validation.validators import EntityRequiredColumnValidator


class FunctionsEntity(Entity):
    def __init__(self) -> None:
        readable_storage = get_storage(StorageKey.FUNCTIONS)
        writable_storage = get_writable_storage(StorageKey.FUNCTIONS_RAW)
        schema = readable_storage.get_schema()

        super().__init__(
            storages=[readable_storage, writable_storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(
                    storage=readable_storage,
                )
            ),
            abstract_column_set=schema.get_columns(),
            join_relationships={},
            writable_storage=writable_storage,
            validators=[
                EntityRequiredColumnValidator({"project_id"}),
            ],
            required_time_column="timestamp",
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            ReferrerRateLimiterProcessor(),
            ProjectReferrerRateLimiter("project_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
            ResourceQuotaProcessor("project_id"),
        ]
