import json
import rapidjson

from datetime import datetime
from typing import Optional, Mapping, Sequence

from snuba.clickhouse import DATETIME_FORMAT
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.processor import MessageProcessor
from snuba.query.extensions import QueryExtension
from snuba.util import escape_col
from snuba.writer import BatchWriter


class Dataset(object):
    """
    A Dataset defines the complete set of data sources, schemas, and
    transformations that are required to:
        - Consume, transform, and insert data payloads from Kafka into Clickhouse.
        - Define how Snuba API queries are transformed into SQL.

    This is the the initial boilerplate. schema and processor will come.
    """

    def __init__(self,
            dataset_schemas: DatasetSchemas,
            *,
            processor: Optional[MessageProcessor],
            default_topic: Optional[str],
            # PR 479 changes the architecture of the dataset
            # allowing a dataset not to have write functions.
            default_replacement_topic: Optional[str] = None,
            default_commit_log_topic: Optional[str] = None):
        self.__dataset_schemas = dataset_schemas
        self.__processor = processor
        self.__default_topic = default_topic
        self.__default_replacement_topic = default_replacement_topic
        self.__default_commit_log_topic = default_commit_log_topic

    def get_dataset_schemas(self) -> DatasetSchemas:
        return self.__dataset_schemas

    def get_processor(self) -> Optional[MessageProcessor]:
        return self.__processor

    def get_writer(self, options=None, table_name=None) -> BatchWriter:
        from snuba import settings
        from snuba.clickhouse.http import HTTPBatchWriter

        def default(value):
            if isinstance(value, datetime):
                return value.strftime(DATETIME_FORMAT)
            else:
                raise TypeError

        return HTTPBatchWriter(
            self.get_dataset_schemas().get_write_schema_enforce(),
            settings.CLICKHOUSE_HOST,
            settings.CLICKHOUSE_HTTP_PORT,
            lambda row: json.dumps(row, default=default).encode("utf-8"),
            options,
            table_name,
        )

    def get_bulk_writer(self, options=None, table_name=None) -> BatchWriter:
        """
        This is a stripped down verison of the writer designed
        for better performance when loading data in bulk.
        """
        # TODO: Consider using rapidjson to encode everywhere
        # once we will be confident it is reliable enough.

        from snuba import settings
        from snuba.clickhouse.http import HTTPBatchWriter

        return HTTPBatchWriter(
            self.get_dataset_schemas().get_write_schema_enforce(),
            settings.CLICKHOUSE_HOST,
            settings.CLICKHOUSE_HTTP_PORT,
            lambda row: rapidjson.dumps(row).encode("utf-8"),
            options,
            table_name,
            chunk_size=settings.BULK_CLICKHOUSE_BUFFER,
        )

    def default_conditions(self):
        """
        Return a list of the default conditions that should be applied to all
        queries on this dataset.
        """
        return []

    def get_default_topic(self) -> Optional[str]:
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
