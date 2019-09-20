import json
import rapidjson

from datetime import datetime

from snuba.clickhouse import DATETIME_FORMAT
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.processor import MessageProcessor
from snuba.writer import BatchWriter


class TableWriter:
    """
    Provides the features needed to write on a Clickhouse table.
    This will evolve into a more comprehensive representation
    of a Clickhouse table. It will inherit more features from
    the dataset when the dataset itself will become an aggregate
    of table storages.
    """

    def __init__(self, write_schema: WritableTableSchema) -> None:
        self.__table_schema = write_schema

    def get_writer(self, options=None, table_name=None) -> BatchWriter:
        from snuba import settings
        from snuba.clickhouse.http import HTTPBatchWriter

        def default(value):
            if isinstance(value, datetime):
                return value.strftime(DATETIME_FORMAT)
            else:
                raise TypeError

        return HTTPBatchWriter(
            self.__table_schema,
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
            self.__table_schema,
            settings.CLICKHOUSE_HOST,
            settings.CLICKHOUSE_HTTP_PORT,
            lambda row: rapidjson.dumps(row).encode("utf-8"),
            options,
            table_name,
            chunk_size=settings.BULK_CLICKHOUSE_BUFFER,
        )

    def get_bulk_loader(self, source, dest_table):
        """
        Returns the instance of the bulk loader to populate the dataset from an
        external source when present.
        """
        raise NotImplementedError


class KafkaFedTableWriter(TableWriter):
    """
    A TableWriter that listens onto a Kafka topic and
    processes the messages.
    """

    def __init__(
        self,
        write_schema: WritableTableSchema,
        processor: MessageProcessor,
        default_topic: str,
    ) -> None:
        super().__init__(write_schema)
        self.__processor = processor
        self.__default_topic = default_topic

    def get_processor(self) -> MessageProcessor:
        return self.__processor

    def get_default_topic(self) -> str:
        return self.__default_topic

    def get_default_replication_factor(self):
        return 1

    def get_default_partitions(self):
        return 1
