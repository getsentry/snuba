from typing import Any, Mapping, NamedTuple, Sequence, Tuple, Union

from snuba.datasets.schemas import ColumnSet
from snuba.datasets.storage import StorageSelector
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
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
    A dataset represent one or multiple entities in the Snuba data model.
    The class is a facade to access the components used to write on the
    data model and to query the entities.
    To query the data model, it provides a schema (for one table or for
    multiple joined tables), query processing features and query exension
    parsing features.
    To write it CAN provide a TableWriter, which has a schema as well and
    provides a way to stream input from different sources and write them to
    Clickhouse.
    """

    def __init__(
        self, *, storage_selector: StorageSelector, abstract_column_set: ColumnSet
    ) -> None:
        self.__storage_selector = storage_selector
        self.__abstract_column_set = abstract_column_set

    def process_condition(self, condition) -> Tuple[str, str, Any]:
        """
        Return a processed condition tuple.
        This enables a dataset to do any parsing/transformations
        a condition before it is added to the query.
        """
        return condition

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        """
        Returns the extensions for this dataset.
        Every extension comes as an instance of QueryExtension.
        The schema tells Snuba how to parse the query.
        The processor actually does query processing for this
        extension.
        """
        raise NotImplementedError("dataset does not support queries")

    def get_split_query_spec(self) -> Union[None, ColumnSplitSpec]:
        """
        Return the parameters to perform the column split of the query.
        """
        return None

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        """
        Returns a series of transformation functions (in the form of QueryProcessor objects)
        that are applied to queries after parsing and before running them on Clickhouse.
        These are applied in sequence in the same order as they are defined and are supposed
        to be stateless.
        """
        return []

    def get_storage_selector(self) -> StorageSelector:
        """
        Returns the component that provides the storage to run the query onto
        during the query execution.
        """
        return self.__storage_selector

    def get_abstract_columnset(self) -> ColumnSet:
        """
        Returns the abstract query schema for this dataset. This is where Entities
        will come into play since this method will return the structure of the
        data model.
        Now the data model is flat so this is just a simple Schema object. With entities
        this will be a more complex data structure that defines the schema for each entity
        and
        """
        return self.__abstract_column_set


class TimeSeriesDataset(Dataset):
    def __init__(
        self,
        storage_selector: StorageSelector,
        abstract_column_set: ColumnSet,
        time_group_columns: Mapping[str, str],
        time_parse_columns: Sequence[str],
        **kwargs,
    ) -> None:
        super().__init__(
            dataset_schemas=storage_selector,
            abstract_column_set=abstract_column_set,
            **kwargs,
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
