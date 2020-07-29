from typing import Any, Mapping, Optional, Sequence, Tuple

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.escaping import escape_identifier
from snuba.datasets.plans.query_plan import ClickhouseQueryPlanBuilder
from snuba.datasets.storage import Storage, WritableStorage, WritableTableStorage
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.parsing import ParsingContext
from snuba.query.processors import QueryProcessor
from snuba.query.validation import ExpressionValidator
from snuba.util import parse_datetime, qualified_column


class Dataset(object):
    """
    A dataset represents a data model we can run a Snuba Query on.
    A data model provides a logical schema (today it is a flat table,
    soon it will be a graph of Entities).
    The dataset (later the Entity) has access to multiple Storage objects, which
    represent the physical data model. Each one represents a table/view on the
    DB we can query.
    The class is a facade to access the components used to write on the
    data model and to query the entities.

    The dataset is made of several Storage objects (later we will introduce
    entities between Dataset and Storage). Each storage represent a table/view
    we can query.

    When processing a query, there are three main steps:
    - dataset query processing. A series of QueryProcessors are applied to the
      query before deciding which Storage to use. These processors are defined
      by the dataset
    - the Storage to run the query onto is selected and the query is transformed
      into a Clickhouse Query. This is done by a ClickhouseQueryPlanBuilder. This object
      produces a plan that includes the Query contextualized on the storage/s, the
      list of processors to apply and the strategy to run the query (in case of
      any strategy more complex than a single DB query like a split).
    - storage query processing. A second series of QueryProcessors are applied
      to the query. These are defined by the storage.

    The architecture of the Dataset is divided in two layers. The highest layer
    provides the logic we use to deal with the data model. (writers, query processors,
    query planners, etc.). The lowest layer incldues simple objects that define
    the query itself (Query, Schema, RelationalSource). The lop layer object access and
    manipulate the lower layer objects.
    """

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
        self.__abstract_column_set = abstract_column_set
        self.__writable_storage = writable_storage

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        """
        Returns the extensions for this dataset.
        Every extension comes as an instance of QueryExtension.
        The schema tells Snuba how to parse the query.
        The processor actually does query processing for this extension.
        """
        raise NotImplementedError("dataset does not support queries")

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        """
        Returns a series of transformation functions (in the form of QueryProcessor objects)
        that are applied to queries after parsing and before running them on the storage.
        These are applied in sequence in the same order as they are defined and are supposed
        to be stateless.
        """
        return []

    def get_abstract_columnset(self) -> ColumnSet:
        """
        Returns the abstract query schema for this dataset. This is where Entities
        will come into play since this method will return the structure of the
        data model.
        Now the data model is flat so this is just a simple ColumnSet object. With entities
        this will be a more complex data structure that defines the schema for each entity
        and their relations.
        """
        # TODO: Make this available to the dataset query processors.
        return self.__abstract_column_set

    def get_query_plan_builder(self) -> ClickhouseQueryPlanBuilder:
        """
        Returns the component that transforms a Snuba query in a Storage query by selecting
        the storage and provides the directions on how to run the query.
        """
        return self.__query_plan_builder

    def get_all_storages(self) -> Sequence[Storage]:
        """
        Returns all storages for this dataset.
        This method should be used for schema bootstrap and migrations.
        It is not supposed to be used during query processing.
        """
        return self.__storages

    def get_writable_storage(self) -> Optional[WritableTableStorage]:
        """
        Temporarily support getting the writable storage from a dataset.
        Once consumers/replacers no longer reference datasets, this can be removed
        and datasets can have more than one writable storage.
        """
        return self.__writable_storage

    # Old methods that we are migrating away from
    def column_expr(
        self,
        column_name,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ):
        """
        Return an expression for the column name. Handle special column aliases
        that evaluate to something else.
        """
        return escape_identifier(qualified_column(column_name, table_alias))

    def process_condition(self, condition) -> Tuple[str, str, Any]:
        """
        Return a processed condition tuple.
        This enables a dataset to do any parsing/transformations
        a condition before it is added to the query.
        """
        return condition

    def get_expression_validators(self) -> Sequence[ExpressionValidator]:
        """
        Provides the a sequence of expression validators for this dataset.
        The typical use case is validating that calls to dataset specific
        functions are well formed.
        """
        return []


class TimeSeriesDataset(Dataset):
    def __init__(
        self,
        *,
        storages: Sequence[Storage],
        query_plan_builder: ClickhouseQueryPlanBuilder,
        abstract_column_set: ColumnSet,
        writable_storage: Optional[WritableStorage],
        time_group_columns: Mapping[str, str],
        time_parse_columns: Sequence[str],
    ) -> None:
        super().__init__(
            storages=storages,
            query_plan_builder=query_plan_builder,
            abstract_column_set=abstract_column_set,
            writable_storage=writable_storage,
        )
        # Convenience columns that evaluate to a bucketed time. The bucketing
        # depends on the granularity parameter.
        # The bucketed time column names cannot be overlapping with existing
        # schema columns
        for bucketed_column in time_group_columns.keys():
            assert (
                bucketed_column not in abstract_column_set
            ), f"Bucketed column {bucketed_column} is already defined in the schema"
        self.__time_group_columns = time_group_columns
        self.__time_parse_columns = time_parse_columns

    def time_expr(self, column_name: str, granularity: int, table_alias: str) -> str:
        real_column = qualified_column(column_name, table_alias)
        template = {
            3600: "toStartOfHour({column}, 'Universal')",
            60: "toStartOfMinute({column}, 'Universal')",
            86400: "toDate({column}, 'Universal')",
        }.get(
            granularity,
            "toDateTime(multiply(intDiv(toUInt32({column}), {granularity}), {granularity}), 'Universal')",
        )
        return template.format(column=real_column, granularity=granularity)

    def column_expr(
        self,
        column_name,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ):
        # We want to permit functions here, so we need to make sure we're not trying
        # to look up lists in the dictionary or it will fail with a type error.
        if isinstance(column_name, str) and column_name in self.__time_group_columns:
            real_column = self.__time_group_columns[column_name]
            return self.time_expr(real_column, query.get_granularity(), table_alias)
        else:
            return super().column_expr(column_name, query, parsing_context, table_alias)

    def process_condition(self, condition) -> Tuple[str, str, Any]:
        lhs, op, lit = condition
        if (
            lhs in self.__time_parse_columns
            and op in (">", "<", ">=", "<=", "=", "!=")
            and isinstance(lit, str)
        ):
            lit = parse_datetime(lit)
        return lhs, op, lit
