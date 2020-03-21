from dataclasses import dataclass
import json
import rapidjson

from datetime import datetime
from typing import Optional, Sequence

from snuba import settings
from snuba.clickhouse import DATETIME_FORMAT
from snuba.clickhouse.config import ClickhouseConnectionConfig
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.processor import MessageProcessor
from snuba.replacers.replacer_processor import ReplacerProcessor
from snuba.snapshots.loaders import BulkLoader
from snuba.writer import BatchWriter


@dataclass(frozen=True)
class KafkaTopicSpec:
    topic_name: str
    partitions_number: int
    replication_factor: int = 1


class KafkaStreamLoader:
    """
    This class is a stream loader for a TableWriter. It provides what we need
    to start a Kafka consumer to fill in the table.
    """

    def __init__(
        self,
        processor: MessageProcessor,
        default_topic: str,
        replacement_topic: Optional[str] = None,
        commit_log_topic: Optional[str] = None,
    ) -> None:
        self.__processor = processor
        self.__default_topic_spec = KafkaTopicSpec(
            topic_name=default_topic,
            partitions_number=settings.TOPIC_PARTITION_COUNTS.get(default_topic, 1),
        )
        self.__replacement_topic_spec = (
            KafkaTopicSpec(
                topic_name=replacement_topic,
                partitions_number=settings.TOPIC_PARTITION_COUNTS.get(
                    replacement_topic, 1
                ),
            )
            if replacement_topic
            else None
        )
        self.__commit_log_topic_spec = (
            KafkaTopicSpec(
                topic_name=commit_log_topic,
                partitions_number=settings.TOPIC_PARTITION_COUNTS.get(
                    commit_log_topic, 1
                ),
            )
            if commit_log_topic
            else None
        )

    def get_processor(self) -> MessageProcessor:
        return self.__processor

    def get_default_topic_spec(self) -> KafkaTopicSpec:
        return self.__default_topic_spec

    def get_replacement_topic_spec(self) -> Optional[KafkaTopicSpec]:
        return self.__replacement_topic_spec

    def get_commit_log_topic_spec(self) -> Optional[KafkaTopicSpec]:
        return self.__commit_log_topic_spec

    def get_all_topic_specs(self) -> Sequence[KafkaTopicSpec]:
        ret = [self.__default_topic_spec]
        if self.__replacement_topic_spec:
            ret.append(self.__replacement_topic_spec)
        if self.__commit_log_topic_spec:
            ret.append(self.__commit_log_topic_spec)
        return ret


class TableWriter:
    """
    This class provides to a dataset write support on a Clickhouse table.
    It is schema aware (the Clickhouse write schema), it provides a writer
    to write on Clickhouse and a two loaders: one for bulk load of the table
    and the other for streaming load.

    Eventually, after some heavier refactoring of the consumer scripts,
    we could make the writing process more abstract and hide in this class
    the streaming, processing and writing. The writer in such architecture
    could coordinate the ingestion process but that requires a reshuffle
    of responsibilities in the consumer scripts and a common interface
    between bulk load and stream load.
    """

    def __init__(
        self,
        write_schema: WritableTableSchema,
        stream_loader: KafkaStreamLoader,
        clickhouse_connection_config: ClickhouseConnectionConfig,
        replacer_processor: Optional[ReplacerProcessor] = None,
    ) -> None:
        self.__table_schema = write_schema
        self.__stream_loader = stream_loader
        self.__clickhouse_connection_config = clickhouse_connection_config
        self.__replacer_processor = replacer_processor

    def get_schema(self) -> WritableTableSchema:
        return self.__table_schema

    def get_writer(
        self, options=None, table_name=None, rapidjson_serialize=False
    ) -> BatchWriter:
        from snuba import settings
        from snuba.clickhouse.http import HTTPBatchWriter

        def default(value):
            if isinstance(value, datetime):
                return value.strftime(DATETIME_FORMAT)
            else:
                raise TypeError

        return HTTPBatchWriter(
            self.__table_schema,
            self.__clickhouse_connection_config.host,
            self.__clickhouse_connection_config.http_port,
            lambda row: (
                rapidjson.dumps(row, default=default)
                if rapidjson_serialize
                else json.dumps(row, default=default)
            ).encode("utf-8"),
            options,
            table_name,
            chunk_size=settings.CLICKHOUSE_HTTP_CHUNK_SIZE,
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
            self.__clickhouse_connection_config.host,
            self.__clickhouse_connection_config.http_port,
            lambda row: rapidjson.dumps(row).encode("utf-8"),
            options,
            table_name,
            chunk_size=settings.BULK_CLICKHOUSE_BUFFER,
        )

    def get_bulk_loader(self, source, dest_table) -> BulkLoader:
        """
        Returns the instance of the bulk loader to populate the dataset from an
        external source when present.
        """
        raise NotImplementedError

    def get_stream_loader(self) -> KafkaStreamLoader:
        return self.__stream_loader

    def get_replacer_processor(self) -> Optional[ReplacerProcessor]:
        """
        Returns a replacement processor if this table writer knows how to do
        replacements on the table it manages.
        """
        return self.__replacer_processor

    def get_clickhouse_connection_config(self) -> ClickhouseConnectionConfig:
        return self.__clickhouse_connection_config
