from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from snuba.clickhouse.columns import Column
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entity import Entity
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storage import ReadableTableStorage, Storage, WritableTableStorage
from snuba.pipeline.query_pipeline import QueryPipelineBuilder
from snuba.query.data_source.join import JoinRelationship
from snuba.query.processors import QueryProcessor
from snuba.query.validation import FunctionCallValidator
from snuba.query.validation.validators import QueryValidator
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

    name: str
    query_processors: Sequence[QueryProcessor]
    columns: Sequence[Column[SchemaModifiers]]
    readable_storage: ReadableTableStorage
    validators: Sequence[QueryValidator]
    translation_mappers: TranslationMappers
    required_time_column: str
    writeable_storage: Optional[WritableTableStorage] = None
    join_relationships: Mapping[str, JoinRelationship] = field(default_factory=dict)
    function_call_validators: Mapping[str, FunctionCallValidator] = field(
        default_factory=dict
    )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return self.query_processors

    def get_data_model(self) -> EntityColumnSet:
        return EntityColumnSet(self.columns)

    def get_join_relationship(self, relationship: str) -> Optional[JoinRelationship]:
        return self.join_relationships.get(relationship)

    def get_all_join_relationships(self) -> Mapping[str, JoinRelationship]:
        return self.join_relationships

    def get_query_pipeline_builder(self) -> QueryPipelineBuilder[ClickhouseQueryPlan]:
        from snuba.pipeline.simple_pipeline import SimplePipelineBuilder

        return SimplePipelineBuilder(
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=self.readable_storage, mappers=self.translation_mappers
            )
        )

    def get_all_storages(self) -> Sequence[Storage]:
        return (
            [self.readable_storage]
            if not self.writeable_storage
            else [self.readable_storage, self.writeable_storage]
        )

    def get_function_call_validators(self) -> Mapping[str, FunctionCallValidator]:
        return self.function_call_validators

    def get_validators(self) -> Sequence[QueryValidator]:
        return self.validators

    def get_writable_storage(self) -> Optional[WritableTableStorage]:
        return self.writeable_storage

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PluggableEntity) and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)
