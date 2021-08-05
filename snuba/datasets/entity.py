from abc import ABC, abstractmethod
from typing import Mapping, Optional, Sequence, Set

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.storage import Storage, WritableTableStorage
from snuba.pipeline.query_pipeline import QueryPipelineBuilder
from snuba.query.data_source.join import JoinRelationship
from snuba.query.extensions import QueryExtension
from snuba.query.functions import GLOBAL_VALID_FUNCTIONS
from snuba.query.processors import QueryProcessor
from snuba.query.validation import FunctionCallValidator
from snuba.query.validation.validators import QueryValidator
from snuba.utils.describer import Describable, Description, Property


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
    ) -> None:
        self.__storages = storages
        self.__query_pipeline_builder = query_pipeline_builder
        self.__writable_storage = writable_storage
        self.__data_model = abstract_column_set
        self.__join_relationships = join_relationships
        self.__validators = validators
        self.required_time_column = required_time_column

    @abstractmethod
    def get_extensions(self) -> Mapping[str, QueryExtension]:
        """
        Returns the extensions for this entity.
        Every extension comes as an instance of QueryExtension.
        The schema tells Snuba how to parse the query.
        The processor actually does query processing for this extension.
        """
        # TODO: How does this work with JOINs?
        raise NotImplementedError("entity does not support queries")

    @abstractmethod
    def get_query_processors(self) -> Sequence[QueryProcessor]:
        """
        Returns a series of transformation functions (in the form of QueryProcessor objects)
        that are applied to queries after parsing and before running them on the storage.
        These are applied in sequence in the same order as they are defined and are supposed
        to be stateless.
        """
        return []

    def get_data_model(self) -> ColumnSet:
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

    def get_valid_functions(self) -> Set[str]:
        """
        Returns valid functions names for an entity.
        Defaults to set of functions that are valid for every entity.
        """
        return GLOBAL_VALID_FUNCTIONS

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
        return self.__validators if self.__validators is not None else []

    def get_writable_storage(self) -> Optional[WritableTableStorage]:
        """
        Temporarily support getting the writable storage from an entity.
        Once consumers/replacers no longer reference entity, this can be removed
        and entity can have more than one writable storage.
        """
        return self.__writable_storage

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
