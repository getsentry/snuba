from abc import ABC
from typing import Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors import QueryProcessor
from snuba.query.processors.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
    ProjectRateLimiterProcessor,
)

profile_columns = EntityColumnSet(
    [
        Column("organization_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("transaction_id", UUID()),
        Column("received", DateTime()),
        Column("profile", String()),
        Column("symbols", String()),
        Column("android_api_level", UInt(32, Modifiers(nullable=True))),
        Column("device_classification", String()),
        Column("device_locale", String()),
        Column("device_manufacturer", String()),
        Column("device_model", String()),
        Column("device_os_build_number", String()),
        Column("device_os_name", String()),
        Column("device_os_version", String()),
        Column("duration_ns", UInt(64)),
        Column("environment", String(Modifiers(nullable=True))),
        Column("platform", String()),
        Column("trace_id", UUID()),
        Column("transaction_name", String()),
        Column("version_name", String()),
        Column("version_code", String()),
    ]
)


class ProfilesEntity(Entity, ABC):
    def __init__(self,) -> None:
        writable_storage = get_writable_storage(StorageKey.PROFILES)
        readable_storage = get_storage(StorageKey.PROFILES)

        super().__init__(
            storages=[writable_storage, readable_storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(readable_storage)
            ),
            abstract_column_set=profile_columns,
            join_relationships={},
            writable_storage=writable_storage,
            validators=[],
            required_time_column="received",
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            OrganizationRateLimiterProcessor(org_column="organization_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
        ]
