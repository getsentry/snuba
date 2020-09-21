from typing import Any, Mapping, Optional, Sequence, Tuple, Union

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.escaping import escape_identifier
from snuba.datasets.plans.query_plan import ClickhouseQueryPlanBuilder
from snuba.datasets.storage import Storage, WritableStorage, WritableTableStorage
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.parsing import ParsingContext
from snuba.query.processors import QueryProcessor
from snuba.util import qualified_column


class Entity(object):
    def __init__(
        self,
        *,
        storages: Sequence[Storage],
        query_plan_builder: ClickhouseQueryPlanBuilder,
        abstract_column_set: ColumnSet,
        writable_storage: Optional[WritableStorage],
    ) -> None:
        self.__storages = storages
        self.__query_plan_builder = query_plan_builder
        self.__writable_storage = writable_storage
        # TODO: This data model will change as we add more functionality
        self.__data_model = abstract_column_set

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        """
        Returns the extensions for this entity.
        Every extension comes as an instance of QueryExtension.
        The schema tells Snuba how to parse the query.
        The processor actually does query processing for this extension.
        """
        raise NotImplementedError("entity does not support queries")

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

    def get_query_plan_builder(self) -> ClickhouseQueryPlanBuilder:
        """
        Returns the component that transforms a Snuba query in a Storage query by selecting
        the storage and provides the directions on how to run the query.
        """
        return self.__query_plan_builder

    def get_all_storages(self) -> Sequence[Storage]:
        """
        Returns all storages for this entity.
        This method should be used for schema bootstrap and migrations.
        It is not supposed to be used during query processing.
        """
        return self.__storages

    # TODO: I just copied this over because I haven't investigated what it does. It can
    # probably be refactored/removed but I need to dig into it.
    def get_writable_storage(self) -> Optional[WritableTableStorage]:
        """
        Temporarily support getting the writable storage from an entity.
        Once consumers/replacers no longer reference entity, this can be removed
        and entity can have more than one writable storage.
        """
        # TODO: mypy complains here about WritableStorage vs WritableTableStorage.
        return self.__writable_storage

    # DEPRECATED: Should move to translations/processors
    def column_expr(
        self,
        column_name: str,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ) -> Union[None, Any]:
        """
        Return an expression for the column name. Handle special column aliases
        that evaluate to something else.
        """
        return escape_identifier(qualified_column(column_name, table_alias))

    def process_condition(
        self, condition: Tuple[str, str, Any]
    ) -> Tuple[str, str, Any]:
        """
        Return a processed condition tuple.
        This enables a dataset to do any parsing/transformations
        a condition before it is added to the query.
        """
        return condition
