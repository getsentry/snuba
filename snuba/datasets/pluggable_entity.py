from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Mapping, Optional, Sequence

from snuba.clickhouse.columns import Column
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entity import Entity
from snuba.datasets.entity_subscriptions.processors import EntitySubscriptionProcessor
from snuba.datasets.entity_subscriptions.validators import EntitySubscriptionValidator
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    ClickhouseQueryPlanBuilder,
)
from snuba.datasets.plans.storage_plan_builder import StorageQueryPlanBuilder
from snuba.datasets.storage import (
    EntityStorageConnection,
    Storage,
    WritableTableStorage,
)
from snuba.pipeline.query_pipeline import QueryPipelineBuilder
from snuba.query.data_source.join import JoinRelationship
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.validation import FunctionCallValidator
from snuba.query.validation.validators import (
    ColumnValidationMode,
    EntityContainsColumnsValidator,
    IllegalAggregateInConditionValidator,
    QueryValidator,
)
from snuba.utils.schemas import SchemaModifiers


@dataclass
class PluggableEntity(Entity):
    """
    PluggableEntity is a version of Entity that is designed to be populated by
    static YAML-based configuration files. It is intentionally less flexible
    than Entity. See the documentation of Entity for explanation about how
    overridden methods are supposed to behave.

    At the time of writing, it makes certain assumptions, specifically that
    the query pipeline builder will map all queries to a single storage, the
    provided readable storage.
    """

    entity_key: EntityKey
    storages: List[EntityStorageConnection]
    query_processors: Sequence[LogicalQueryProcessor]
    columns: Sequence[Column[SchemaModifiers]]
    validators: Sequence[QueryValidator]
    required_time_column: Optional[str]
    storage_selector: QueryStorageSelector
    validate_data_model: ColumnValidationMode | None = None
    join_relationships: Mapping[str, JoinRelationship] = field(default_factory=dict)
    function_call_validators: Mapping[str, FunctionCallValidator] = field(
        default_factory=dict
    )
    # partition_key_column_name is used in data slicing (the value in this storage column
    # will be used to "choose" slices)
    partition_key_column_name: Optional[str] = None
    subscription_processors: Optional[Sequence[EntitySubscriptionProcessor]] = None
    subscription_validators: Optional[Sequence[EntitySubscriptionValidator]] = None

    def _get_builtin_validators(self) -> Sequence[QueryValidator]:
        mappers = [s.translation_mappers for s in self.storages]
        return [
            EntityContainsColumnsValidator(
                EntityColumnSet(self.columns),
                mappers,
                self.validate_data_model or ColumnValidationMode.ERROR,
            ),
            IllegalAggregateInConditionValidator(),
        ]

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return self.query_processors

    def get_data_model(self) -> EntityColumnSet:
        return EntityColumnSet(self.columns)

    def get_join_relationship(self, relationship: str) -> Optional[JoinRelationship]:
        return self.join_relationships.get(relationship)

    def get_all_join_relationships(self) -> Mapping[str, JoinRelationship]:
        return self.join_relationships

    def get_query_pipeline_builder(self) -> QueryPipelineBuilder[ClickhouseQueryPlan]:
        from snuba.pipeline.simple_pipeline import SimplePipelineBuilder

        query_plan_builder: ClickhouseQueryPlanBuilder = StorageQueryPlanBuilder(
            storages=self.storages,
            selector=self.storage_selector,
            partition_key_column_name=self.partition_key_column_name,
        )

        return SimplePipelineBuilder(query_plan_builder=query_plan_builder)

    def get_all_storages(self) -> Sequence[Storage]:
        return [storage_connection.storage for storage_connection in self.storages]

    def get_all_storage_connections(self) -> Sequence[EntityStorageConnection]:
        return self.storages

    def get_writable_storage(self) -> Optional[WritableTableStorage]:
        for storage_connection in self.storages:
            if storage_connection.is_writable and isinstance(
                storage_connection.storage, WritableTableStorage
            ):
                return storage_connection.storage
        return None

    def get_storage_selector(self) -> Optional[QueryStorageSelector]:
        return self.storage_selector

    def get_function_call_validators(self) -> Mapping[str, FunctionCallValidator]:
        return self.function_call_validators

    def get_validators(self) -> Sequence[QueryValidator]:
        return [*self.validators, *self._get_builtin_validators()]

    def get_subscription_processors(
        self,
    ) -> Optional[Sequence[EntitySubscriptionProcessor]]:
        return self.subscription_processors

    def get_subscription_validators(
        self,
    ) -> Optional[Sequence[EntitySubscriptionValidator]]:
        return self.subscription_validators

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, PluggableEntity) and self.entity_key == other.entity_key
        )

    def __hash__(self) -> int:
        return hash(self.entity_key)
