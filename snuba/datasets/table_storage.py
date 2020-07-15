from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

import rapidjson

from snuba import settings
from snuba.clickhouse import DATETIME_FORMAT
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseWriterOptions,
)
from snuba.datasets.message_filters import StreamMessageFilter
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.processor import MessageProcessor
from snuba.replacers.replacer_processor import ReplacerProcessor
from snuba.snapshots import BulkLoadSource
from snuba.snapshots.loaders import BulkLoader
from snuba.snapshots.loaders.single_table import RowProcessor, SingleTableBulkLoader
from snuba.utils.streams.kafka import KafkaPayload
from snuba.writer import BatchWriter, WriterTableRow


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
        pre_filter: Optional[StreamMessageFilter[KafkaPayload]] = None,
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
        self.__pre_filter = pre_filter

    def get_processor(self) -> MessageProcessor:
        return self.__processor

    def get_pre_filter(self) -> Optional[StreamMessageFilter[KafkaPayload]]:
        """
        Returns a filter (or none if none is defined) to be applied to the messages
        coming from the Kafka stream before parsing the content of the message.
        """
        return self.__pre_filter

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
    This class provides to a storage write support on a Clickhouse table.
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
        cluster: ClickhouseCluster,
        write_schema: WritableTableSchema,
        stream_loader: KafkaStreamLoader,
        replacer_processor: Optional[ReplacerProcessor] = None,
        writer_options: ClickhouseWriterOptions = None,
    ) -> None:
        self.__cluster = cluster
        self.__table_schema = write_schema
        self.__stream_loader = stream_loader
        self.__replacer_processor = replacer_processor
        self.__writer_options = writer_options

    def get_schema(self) -> WritableTableSchema:
        return self.__table_schema

    def get_writer(self, options=None, table_name=None) -> BatchWriter[WriterTableRow]:
        from snuba import settings

        def default(value):
            if isinstance(value, datetime):
                return value.strftime(DATETIME_FORMAT)
            else:
                raise TypeError

        table_name = table_name or self.__table_schema.get_table_name()

        options = self.__update_writer_options(options)

        return self.__cluster.get_writer(
            table_name,
            lambda row: rapidjson.dumps(row, default=default).encode("utf-8"),
            options,
            chunk_size=settings.CLICKHOUSE_HTTP_CHUNK_SIZE,
        )

    def get_bulk_writer(
        self, options=None, table_name=None
    ) -> BatchWriter[WriterTableRow]:
        """
        This is a stripped down verison of the writer designed
        for better performance when loading data in bulk.
        """
        # TODO: Consider using rapidjson to encode everywhere
        # once we will be confident it is reliable enough.

        from snuba import settings

        table_name = table_name or self.__table_schema.get_table_name()

        options = self.__update_writer_options(options)

        return self.__cluster.get_writer(
            table_name,
            lambda row: rapidjson.dumps(row).encode("utf-8"),
            options,
            chunk_size=settings.BULK_CLICKHOUSE_BUFFER,
        )

    def get_bulk_loader(
        self,
        source: BulkLoadSource,
        source_table: str,
        dest_table: str,
        row_processor: RowProcessor,
    ) -> BulkLoader:
        """
        Returns the instance of the bulk loader to populate the dataset from an
        external source when present.
        """
        return SingleTableBulkLoader(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            row_processor=row_processor,
            clickhouse=self.__cluster.get_query_connection(
                ClickhouseClientSettings.QUERY
            ),
        )

    def get_stream_loader(self) -> KafkaStreamLoader:
        return self.__stream_loader

    def get_replacer_processor(self) -> Optional[ReplacerProcessor]:
        """
        Returns a replacement processor if this table writer knows how to do
        replacements on the table it manages.
        """
        return self.__replacer_processor

    def __update_writer_options(
        self, options: ClickhouseWriterOptions = None,
    ) -> Mapping[str, Any]:
        if options is None:
            options = {}
        if self.__writer_options:
            return {**options, **self.__writer_options}
        return options
