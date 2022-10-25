from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence, Type, Union

from snuba.datasets.entities.entity_data_model import EntityColumnSet

# from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.storage import Storage, WritableTableStorage
from snuba.pipeline.query_pipeline import QueryPipelineBuilder
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.join import JoinRelationship
from snuba.query.data_source.simple import Entity as EntityDS
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.validation import FunctionCallValidator
from snuba.query.validation.validators import (  # NoTimeBasedConditionValidator,
    ColumnValidationMode,
    EntityContainsColumnsValidator,
    QueryValidator,
    SubscriptionAllowedClausesValidator,
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
        storages: Sequence[Storage],
        query_pipeline_builder: QueryPipelineBuilder[ClickhouseQueryPlan],
        abstract_column_set: ColumnSet,
        join_relationships: Mapping[str, JoinRelationship],
        writable_storage: Optional[WritableTableStorage],
        validators: Optional[Sequence[QueryValidator]],
        required_time_column: Optional[str],
        validate_data_model: ColumnValidationMode = ColumnValidationMode.DO_NOTHING,
        entity_subscription: Optional[Type[EntitySubscription]],
    ) -> None:
        self.__storages = storages
        self.__query_pipeline_builder = query_pipeline_builder
        self.__writable_storage = writable_storage

        # Eventually, the EntityColumnSet should be passed in
        # For now, just convert it so we have the right
        # type from here on
        self.__data_model = EntityColumnSet(abstract_column_set.columns)

        self.__join_relationships = join_relationships
        self.required_time_column = required_time_column

        columns_exist_validator = EntityContainsColumnsValidator(
            self.__data_model, validation_mode=validate_data_model
        )
        self.__validators = (
            [*validators, columns_exist_validator]
            if validators is not None
            else [columns_exist_validator]
        )
        self.__entity_subscription = entity_subscription

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
        Returns all storages for this entity.
        This method should be used for schema bootstrap and migrations.
        It is not supposed to be used during query processing.
        """
        return self.__storages

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

    def get_writable_storage(self) -> Optional[WritableTableStorage]:
        """
        Temporarily support getting the writable storage from an entity.
        Once consumers/replacers no longer reference entity, this can be removed
        and entity can have more than one writable storage.
        """
        return self.__writable_storage

    def get_entity_subscription(self) -> Optional[Type[EntitySubscription]]:
        """
        Returns the entity subscription associated with an entity
        """
        return self.__entity_subscription

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


class InvalidSubscriptionError(Exception):
    pass


@dataclass
class EntitySubscription(ABC):
    organization: Optional[int] = None
    max_allowed_aggregations: Optional[int] = None
    disallowed_aggregations: Optional[Sequence[str]] = None

    @abstractmethod
    def get_entity_subscription_conditions_for_snql(
        self, offset: Optional[int] = None
    ) -> Sequence[Expression]:
        """
        Returns a list of extra conditions that are entity specific and required for the
        snql subscriptions
        """
        raise NotImplementedError

    @abstractmethod
    def validate_query(self, query: Union[CompositeQuery[EntityDS], Query]) -> None:
        """
        Applies entity specific validations on query argument passed
        """
        raise NotImplementedError

    @abstractmethod
    def to_dict(self) -> Mapping[str, Any]:
        raise NotImplementedError


class EntitySubscriptionValidation:
    def validate_query(self, query: Union[CompositeQuery[EntityDS], Query]) -> None:
        # TODO: Support composite queries with multiple entities.
        from_clause = query.get_from_clause()
        if not isinstance(from_clause, EntityDS):
            raise InvalidSubscriptionError("Only simple queries are supported")
        # entity = get_entity(from_clause.key)

        SubscriptionAllowedClausesValidator(
            self.max_allowed_aggregations, self.disallowed_aggregations
        ).validate(query)
        # if entity.required_time_column:
        #     NoTimeBasedConditionValidator(entity.required_time_column).validate(query)


class BaseEntitySubscription(EntitySubscriptionValidation, EntitySubscription):
    def __init__(self, data_dict: Mapping[str, Any] = {}) -> None:
        if "organization" in data_dict:
            try:
                self.organization: int = data_dict["organization"]
            except KeyError:
                raise InvalidQueryException(
                    "organization param is required for any query over sessions entity"
                )

    def get_entity_subscription_conditions_for_snql(
        self, offset: Optional[int] = None
    ) -> Sequence[Expression]:
        if self.organization:
            return [
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "org_id"),
                    Literal(None, self.organization),
                ),
            ]
        return []

    def to_dict(self) -> Mapping[str, Any]:
        if self.organization:
            return {"organization": self.organization}
        return {}
