from abc import ABC, abstractmethod
from typing import Mapping, Optional, Sequence

from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entity_subscriptions.processors import EntitySubscriptionProcessor
from snuba.datasets.entity_subscriptions.validators import EntitySubscriptionValidator
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
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
    QueryValidator,
)
from snuba.utils.describer import Describable, Description, Property
from snuba.utils.schemas import ColumnSet


class Entity(Describable, ABC):
    """
    The Entity has access to multiple Storage objects, which represent the physical
    data model. Each one represents a table/view on the DB we can query.
    """

    def __init__(
        self,
        *,
        storages: Sequence[EntityStorageConnection],
        query_pipeline_builder: QueryPipelineBuilder[ClickhouseQueryPlan],
        abstract_column_set: ColumnSet,
        join_relationships: Mapping[str, JoinRelationship],
        validators: Optional[Sequence[QueryValidator]],
        required_time_column: Optional[str],
        validate_data_model: ColumnValidationMode = ColumnValidationMode.DO_NOTHING,
        subscription_processors: Optional[Sequence[EntitySubscriptionProcessor]],
        subscription_validators: Optional[Sequence[EntitySubscriptionValidator]],
    ) -> None:
        self.__storages = storages
        self.__query_pipeline_builder = query_pipeline_builder

        # Eventually, the EntityColumnSet should be passed in
        # For now, just convert it so we have the right
        # type from here on
        self.__data_model = EntityColumnSet(abstract_column_set.columns)

        self.__join_relationships = join_relationships
        self.__subscription_processors = subscription_processors
        self.__subscription_validators = subscription_validators
        self.required_time_column = required_time_column

        mappers = [s.translation_mappers for s in storages]
        columns_exist_validator = EntityContainsColumnsValidator(
            self.__data_model, mappers, validation_mode=validate_data_model
        )
        self.__validators = (
            [*validators, columns_exist_validator]
            if validators is not None
            else [columns_exist_validator]
        )

    @abstractmethod
    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        """
        Returns a series of transformation functions (in the form of QueryProcessor objects)
        that are applied to queries after parsing and before running them on the storage.
        These are applied in sequence in the same order as they are defined and are supposed
        to be stateless.
        """
        return []

    def get_data_model(self) -> EntityColumnSet:
        """
        Now the data model is flat so this is just a simple ColumnSet object. We can expand this
        to also include relationships between entities.
        """
        return self.__data_model

    def get_join_relationship(self, relationship: str) -> Optional[JoinRelationship]:
        """
        Fetch the join relationship specified by the relationship string.
        """
        return self.__join_relationships.get(relationship)

    def get_all_join_relationships(self) -> Mapping[str, JoinRelationship]:
        """
        Returns all the join relationships
        """
        return self.__join_relationships

    def get_query_pipeline_builder(self) -> QueryPipelineBuilder[ClickhouseQueryPlan]:
        """
        Returns the component that orchestrates building and running query plans.
        """
        return self.__query_pipeline_builder

    def get_all_storages(self) -> Sequence[Storage]:
        """
        Returns all storage and mappers for this entity.
        This method should be used for schema bootstrap and migrations.
        It is not supposed to be used during query processing.
        """
        return [storage_connection.storage for storage_connection in self.__storages]

    def get_all_storage_connections(self) -> Sequence[EntityStorageConnection]:
        """
        Returns all storage and mappers for this entity.
        """
        return self.__storages

    def get_writable_storage(self) -> Optional[WritableTableStorage]:
        """
        Temporarily support getting the writable storage from an entity.
        Once consumers/replacers no longer reference entity, this can be removed
        and entity can have more than one writable storage.
        """
        for storage_connection in self.__storages:
            if storage_connection.is_writable and isinstance(
                storage_connection.storage, WritableTableStorage
            ):
                return storage_connection.storage
        return None

    def get_function_call_validators(self) -> Mapping[str, FunctionCallValidator]:
        """
        Provides a sequence of function expression validators for
        this entity. The typical use case is the validation that
        calls to entity specific functions are well formed.
        """
        return {}

    def get_validators(self) -> Sequence[QueryValidator]:
        """
        Provides a sequence of QueryValidators that can be used to validate that
        a query using this Entity is correct.

        :return: A sequence of validators.
        :rtype: Sequence[QueryValidator]
        """
        return self.__validators

    def get_subscription_processors(
        self,
    ) -> Optional[Sequence[EntitySubscriptionProcessor]]:
        """
        Provides an entity subscription processors to be run on on subscription queries.
        """
        return self.__subscription_processors

    def get_subscription_validators(
        self,
    ) -> Optional[Sequence[EntitySubscriptionValidator]]:
        """
        Provides an entity subscription validators to be run on on subscription queries.
        """
        return self.__subscription_validators

    def describe(self) -> Description:
        relationships = []
        for name, destination in self.get_all_join_relationships().items():
            relationships.append(
                Description(
                    header=name,
                    content=[
                        Property("Destination", destination.rhs_entity.value),
                        Property("Type", destination.join_type.value),
                        Description(
                            header="Join keys",
                            content=[
                                f"{lhs} = {destination.join_type.value}.{rhs}"
                                for lhs, rhs in destination.columns
                            ],
                        ),
                    ],
                )
            )
        return Description(
            header=None,
            content=[
                Description(
                    header="Entity schema",
                    content=[
                        column.for_schema() for column in self.get_data_model().columns
                    ],
                ),
                Description(header="Relationships", content=relationships),
            ],
        )
