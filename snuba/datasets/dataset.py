from typing import Any, Optional, Mapping, NamedTuple, Sequence, Tuple, Union

from snuba.clickhouse.escaping import escape_identifier
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.table_storage import TableWriter
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.types import Condition
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
        self,
        dataset_schemas: DatasetSchemas,
        *,
        table_writer: Optional[TableWriter] = None,
    ) -> None:
        self.__dataset_schemas = dataset_schemas
        self.__table_writer = table_writer

    def get_dataset_schemas(self) -> DatasetSchemas:
        """
        Returns the collections of schemas for DDL operations and for
        query.
        See TableWriter to get a write schema.
        """
        return self.__dataset_schemas

    def can_write(self) -> bool:
        """
        Returns True if this dataset has write capabilities
        """
        return self.__table_writer is not None

    def get_table_writer(self) -> Optional[TableWriter]:
        """
        Returns the TableWriter or throws if the dataaset is a readonly one.

        Once we will have a full TableStorage implementation this method will
        disappear since we will have a table storage factory that will return
        only writable ones, scripts will depend on table storage instead of
        going through datasets.
        """
        return self.__table_writer

    def default_conditions(self, table_alias: str = "") -> Sequence[Condition]:
        """
        Return a list of the default conditions that should be applied to all
        queries on this dataset.
        """
        return []

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


class TimeSeriesDataset(Dataset):
    def __init__(
        self,
        *args,
        dataset_schemas: DatasetSchemas,
        time_group_columns: Mapping[str, str],
        time_parse_columns: Sequence[str],
        **kwargs,
    ) -> None:
        super().__init__(*args, dataset_schemas=dataset_schemas, **kwargs)
        # Convenience columns that evaluate to a bucketed time. The bucketing
        # depends on the granularity parameter.
        # The bucketed time column names cannot be overlapping with existing
        # schema columns
        read_schema = dataset_schemas.get_read_schema()
        if read_schema:
            for bucketed_column in time_group_columns.keys():
                assert (
                    bucketed_column not in read_schema.get_columns()
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
        if column_name in self.__time_group_columns:
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
