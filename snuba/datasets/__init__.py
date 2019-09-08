from typing import Optional, Mapping, Tuple

from snuba.util import escape_col
from snuba.schemas import RequestSchema, Schema, GENERIC_QUERY_SCHEMA
from snuba.query.query_processor import ExtensionQueryProcessor


class Dataset(object):
    """
    A Dataset defines the complete set of data sources, schemas, and
    transformations that are required to:
        - Consume, transform, and insert data payloads from Kafka into Clickhouse.
        - Define how Snuba API queries are transformed into SQL.

    This is the the initial boilerplate. schema and processor will come.
    """

    def __init__(self, schema, *, processor,
            default_topic: str,
            default_replacement_topic: Optional[str] = None,
            default_commit_log_topic: Optional[str] = None):
        self._schema = schema
        self.__processor = processor
        self.__default_topic = default_topic
        self.__default_replacement_topic = default_replacement_topic
        self.__default_commit_log_topic = default_commit_log_topic

    def get_schema(self):
        return self._schema

    def get_processor(self):
        return self.__processor

    def get_writer(self, options=None, table_name=None):
        from snuba import settings
        from snuba.clickhouse.http import HTTPBatchWriter

        return HTTPBatchWriter(
            self._schema,
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

    def _get_base_schema(self) -> Schema:
        """
        The base schema of the query.
        """
        return GENERIC_QUERY_SCHEMA

    def _get_extensions(self) -> Mapping[str, Tuple[Schema, ExtensionQueryProcessor]]:
        """
        Returns the extensions for this dataset.
        Every extension comes as a tuple (schema, processor).
        The schema tells Snuba how to parse the query.
        The processor actually does query processing for this
        extension.
        """
        raise NotImplementedError('dataset does not support queries')

    def get_query_schema(self) -> RequestSchema:
        base_schema = self._get_base_schema()
        extensions_schemas = {key: val[0] for key, val in self._get_extensions().items()}
        return RequestSchema(
            base_schema,
            extensions_schemas,
        )

    def get_query_processors(self) -> Mapping[str, ExtensionQueryProcessor]:
        return {key: val[1] for key, val in self._get_extensions().items()}


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
