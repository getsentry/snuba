from abc import ABC
from typing import Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, String, UInt
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors import QueryProcessor

columns = EntityColumnSet(
    [
        Column("event_id", UUID()),
        Column("timestamp", DateTime()),
        Column("event_type", String()),
        Column("user", String()),
        Column("details", String()),
        Column("project_id", UInt(64)),
    ]
)


class AuditLogEntity(Entity, ABC):
    def __init__(
        self,
    ) -> None:
        writable_storage = get_writable_storage(StorageKey.AUDIT_LOG)

        super().__init__(
            storages=[writable_storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(writable_storage)
            ),
            abstract_column_set=columns,
            join_relationships={},
            writable_storage=writable_storage,
            validators=[],
            required_time_column="timestamp",
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return []
