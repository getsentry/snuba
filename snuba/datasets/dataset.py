from typing import Any, Mapping, NamedTuple, Optional, Sequence, Tuple, Union

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.config import ClickhouseConnectionConfig
from snuba.clickhouse.escaping import escape_identifier
from snuba.clickhouse.native import NativeDriverReader
from snuba.clickhouse.pool import ClickhousePool
from snuba.clickhouse.query import ClickhouseQuery
from snuba.datasets.storage import QueryStorageSelector, Storage, WritableStorage
from snuba.datasets.table_storage import TableWriter
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.reader import Reader
from snuba.util import parse_datetime, qualified_column


class ColumnSplitSpec(NamedTuple):
    """
    Provides the column names needed to perform column splitting.
    id_column represent the identity of the row.

    """

    id_column: str
    project_column: str
    timestamp_column: str

    def get_min_columns(self) -> Sequence[str]:
        return [self.id_column, self.project_column, self.timestamp_column]


class Dataset(object):
    """
    A dataset represents a data model we can run a Snuba Query on.
    A data model provides an abstract schema (today it is a flat table,
    soon it will be a graph of Entities).
    The dataset (later the Entity) has access to multiple Storage objects,
    each one represents a table/view on the DB we can query.
    The class is a facade to access the components used to write on the
    data model and to query the entities.

    The dataset is made of several Storage objects (later we will introduce
    entities between Dataset and Storage). Each storage represent a table/view
    we can query.
    When processing a query, there are three main steps:
    - dataset query processing. A series of QueryProcessors are applied to the
      query before deciding which Storage to use. These processors are defined
      by the dataset
    - the Storage to run the query onto is selected. This is done by a
      QueryStorageSelector which is provided by the dataset. From this point
      the query processing is storage specific. [This step is temporary till we
      do not have a Query Plan Builder]
    - storage query processing. A second series of QueryProcessors are applied
      to the query. These are defined by the storage.

    The architecture of the Dataset is divided in two layers. The highest layer
    provides the logic we use to deal with the data model. (writers, query processors,
    storage selectors, etc.). The lowest layer incldues simple objects that define
    the query itself (Query, Schema, RelationalSource). The lop layer object access and
    manipulate the lower layer objects.
    """

    def __init__(
        self,
        *,
        storages: Sequence[Storage],
        storage_selector: QueryStorageSelector,
        abstract_column_set: ColumnSet,
        writable_storage: Optional[WritableStorage],
        clickhouse_connection_config: ClickhouseConnectionConfig,
    ) -> None:
        self.__storages = storages
        self.__storage_selector = storage_selector
        self.__abstract_column_set = abstract_column_set
        self.__writable_storage = writable_storage
        self.__clickhouse_connection_config = clickhouse_connection_config
        self.__clickhouse_rw = ClickhousePool(
            clickhouse_connection_config.host,
            clickhouse_connection_config.port
        )
        self.__clickhouse_ro = ClickhousePool(
            clickhouse_connection_config.host,
            clickhouse_connection_config.port,
            client_settings={"readonly": True}
        )
        self.__reader = NativeDriverReader(self.__clickhouse_ro)

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
        that are applied to queries after parsing and before running them on Clickhouse.
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

    def get_query_storage_selector(self) -> QueryStorageSelector:
        """
        Returns the component that provides the storage to run the query onto
        during the query execution.
        """
        return self.__storage_selector

    def get_all_storages(self) -> Sequence[Storage]:
        """
        Returns all storages for this dataset.
        This method should be used for schema bootstrap and migrations.
        It is not supposed to be used during query processing.
        """
        return self.__storages

    def get_table_writer(self) -> Optional[TableWriter]:
        """
        We allow only one table storage we can write onto per dataset as of now.
        This will move to the entity as soon as we have entities, and
        the constraint of one writable storage will drop as soon as the consumers
        start referencing entities and storages instead of datasets.
        """
        return (
            self.__writable_storage.get_table_writer()
            if self.__writable_storage
            else None
        )

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

    def get_split_query_spec(self) -> Union[None, ColumnSplitSpec]:
        """
        Return the parameters to perform the column split of the query.
        """
        return None

    def get_clickhouse_connection_config(self) -> ClickhouseConnectionConfig:
        return self.__clickhouse_connection_config

    def get_clickhouse_rw(self) -> ClickhousePool:
        return self.__clickhouse_rw

    def get_clickhouse_ro(self) -> ClickhousePool:
        return self.__clickhouse_ro

    def get_clickhouse_reader(self) -> Reader[ClickhouseQuery]:
        return self.__reader


class TimeSeriesDataset(Dataset):
    def __init__(
        self,
        *,
        storages: Sequence[Storage],
        storage_selector: QueryStorageSelector,
        abstract_column_set: ColumnSet,
        writable_storage: Optional[WritableStorage],
        time_group_columns: Mapping[str, str],
        time_parse_columns: Sequence[str],
        clickhouse_connection_config: ClickhouseConnectionConfig,
    ) -> None:
        super().__init__(
            storages=storages,
            storage_selector=storage_selector,
            abstract_column_set=abstract_column_set,
            writable_storage=writable_storage,
            clickhouse_connection_config=clickhouse_connection_config,
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
            3600: "toStartOfHour({column})",
            60: "toStartOfMinute({column})",
            86400: "toDate({column})",
        }.get(
            granularity,
            "toDateTime(intDiv(toUInt32({column}), {granularity}) * {granularity})",
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
