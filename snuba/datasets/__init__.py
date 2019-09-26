from typing import Optional, Mapping, Sequence, Tuple

from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.table_storage import TableWriter
from snuba.query.extensions import QueryExtension

from snuba.util import escape_col, parse_datetime


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

    def __init__(self,
            dataset_schemas: DatasetSchemas,
            *,
            table_writer: Optional[TableWriter] = None):
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

    def default_conditions(self):
        """
        Return a list of the default conditions that should be applied to all
        queries on this dataset.
        """
        return []

    def column_expr(self, column_name, body):
        """
        Return an expression for the column name. Handle special column aliases
        that evaluate to something else.
        """
        return escape_col(column_name)

    def process_condition(self, condition) -> Tuple[str, str, any]:
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
        raise NotImplementedError('dataset does not support queries')

    def get_prewhere_keys(self) -> Sequence[str]:
        """
        Returns the keys that will be upgraded from a WHERE condition to a PREWHERE.

        This is an ordered list, from highest priority to lowest priority. So, a column at index 1 will be upgraded
        before a column at index 2. This is relevant when we have a maximum number of prewhere keys.
        """
        return []


class TimeSeriesDataset(Dataset):
    def __init__(self, *args,
            dataset_schemas: DatasetSchemas,
            time_group_columns: Mapping[str, str],
            time_parse_columns: Sequence[str],
            **kwargs):
        super().__init__(*args, dataset_schemas=dataset_schemas, **kwargs)
        # Convenience columns that evaluate to a bucketed time. The bucketing
        # depends on the granularity parameter.
        # The bucketed time column names cannot be overlapping with existing
        # schema columns
        read_schema = dataset_schemas.get_read_schema()
        if read_schema:
            for bucketed_column in time_group_columns.keys():
                assert \
                    bucketed_column not in read_schema.get_columns(), \
                    f"Bucketed column {bucketed_column} is already defined in the schema"
        self.__time_group_columns = time_group_columns
        self.__time_parse_columns = time_parse_columns

    def __time_expr(self, column_name: str, granularity: int) -> str:
        real_column = self.__time_group_columns[column_name]
        template = {
            3600: 'toStartOfHour({column})',
            60: 'toStartOfMinute({column})',
            86400: 'toDate({column})',
        }.get(granularity, 'toDateTime(intDiv(toUInt32({column}), {granularity}) * {granularity})')
        return template.format(column=real_column, granularity=granularity)

    def column_expr(self, column_name, body):
        if column_name in self.__time_group_columns:
            return self.__time_expr(column_name, body['granularity'])
        else:
            return super().column_expr(column_name, body)

    def process_condition(self, condition) -> Tuple[str, str, any]:
        lhs, op, lit = condition
        if (
            lhs in self.__time_parse_columns and
            op in ('>', '<', '>=', '<=', '=', '!=') and
            isinstance(lit, str)
        ):
            lit = parse_datetime(lit)
        return lhs, op, lit
