import json
import rapidjson

from datetime import datetime
from typing import Optional, Iterable, Mapping

from snuba.clickhouse import DATETIME_FORMAT
from snuba.util import escape_col
from snuba.writer import WriterTableRow


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

        def default(value):
            if isinstance(value, datetime):
                return value.strftime(DATETIME_FORMAT)
            else:
                raise TypeError

        def encode(rows: Iterable[WriterTableRow]) -> Iterable[bytes]:
            return map(
                lambda row: json.dumps(row, default=default).encode("utf-8"),
                rows,
            )

        return HTTPBatchWriter(
            self._schema,
            settings.CLICKHOUSE_HOST,
            settings.CLICKHOUSE_HTTP_PORT,
            encode,
            options,
            table_name,
        )

    def get_bulk_writer(self, options=None, table_name=None):
        """
        This is a stripped down verison of the writer designed
        for better performance when loading data in bulk.
        """
        # TODO: Consider using rapidjson to encode everywhere
        # once we will be confident it is reliable enough.

        from snuba import settings
        from snuba.clickhouse.http import HTTPBatchWriter

        def encode(rows: Iterable[WriterTableRow]) -> Iterable[bytes]:
            ret = bytearray()
            for row in rows:
                ret += rapidjson.dumps(row).encode("utf-8")
            return [ret]

        return HTTPBatchWriter(
            self._schema,
            settings.CLICKHOUSE_HOST,
            settings.CLICKHOUSE_HTTP_PORT,
            encode,
            options,
            table_name,
            # When loading data in bulk, chunking data does not help
            # instead it adds a lot of overhead because we are sending
            # more data to Clickhouse.
            chunked=False,
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
