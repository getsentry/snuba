from typing import Optional, Mapping

from snuba.util import escape_col


class Dataset(object):
    """
    A Dataset defines the complete set of data sources, schemas, and
    transformations that are required to:
        - Consume, transform, and insert data payloads from Kafka into Clickhouse.
        - Define how Snuba API queries are transformed into SQL.

    This is the the initial boilerplate. schema and processor will come.
    """

    def __init__(self, write_schema, *, processor,
            default_topic: str,
            default_replacement_topic: Optional[str] = None,
            default_commit_log_topic: Optional[str] = None,
            read_schema = None):
        self.__write_schema = write_schema
        self.__read_schema = read_schema  # optionally have a different read schema
        self.__processor = processor
        self.__default_topic = default_topic
        self.__default_replacement_topic = default_replacement_topic
        self.__default_commit_log_topic = default_commit_log_topic

    def get_read_schema(self):
        if self.__read_schema is None:
            return self.__write_schema

        return self.__read_schema

    def get_write_schema(self):
        return self.__write_schema

    def get_schemas(self):
        schemas = [self.__read_schema, self.__write_schema]
        return [schema for schema in schemas if schema]

    def get_processor(self):
        return self.__processor

    def get_writer(self, options=None, table_name=None):
        from snuba import settings
        from snuba.clickhouse.http import HTTPBatchWriter

        return HTTPBatchWriter(
            self.__write_schema,
            settings.CLICKHOUSE_HOST,
            settings.CLICKHOUSE_HTTP_PORT,
            options,
            table_name,
        )

    def default_conditions(self):
        """
        Return a list of the default conditions that should be applied to all
        queries on this dataset.
        """
        return []

    def get_default_topic(self) -> str:
        return self.__default_topic

    def get_default_replacement_topic(self) -> Optional[str]:
        return self.__default_replacement_topic

    def get_default_commit_log_topic(self) -> Optional[str]:
        return self.__default_commit_log_topic

    def get_default_replication_factor(self):
        return 1

    def get_default_partitions(self):
        return 1

    def column_expr(self, column_name, body):
        """
        Return an expression for the column name. Handle special column aliases
        that evaluate to something else.
        """
        return escape_col(column_name)

    def get_bulk_loader(self, source, dest_table):
        """
        Returns the instance of the bulk loader to populate the dataset from an
        external source when present.
        """
        raise NotImplementedError

    def get_query_schema(self):
        raise NotImplementedError('dataset does not support queries')


class TimeSeriesDataset(Dataset):
    def __init__(self, *args, time_group_columns: Mapping[str, str], **kwargs):
        super().__init__(*args, **kwargs)
        # Convenience columns that evaluate to a bucketed time. The bucketing
        # depends on the granularity parameter.
        self.__time_group_columns = time_group_columns

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
