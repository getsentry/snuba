from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from snuba import settings
from snuba.clickhouse.http import JSONRow
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseWriterOptions,
)
from snuba.datasets.message_filters import StreamMessageFilter
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storages import StorageKey
from snuba.processor import MessageProcessor
from snuba.replacers.replacer_processor import ReplacerProcessor
from snuba.snapshots import BulkLoadSource
from snuba.snapshots.loaders import BulkLoader
from snuba.snapshots.loaders.single_table import RowProcessor, SingleTableBulkLoader
from snuba.utils.metrics import MetricsBackend
from snuba.utils.streams.backends.kafka import KafkaPayload
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
        default_topic_spec: KafkaTopicSpec,
        pre_filter: Optional[StreamMessageFilter[KafkaPayload]] = None,
        replacement_topic_spec: Optional[KafkaTopicSpec] = None,
        commit_log_topic_spec: Optional[KafkaTopicSpec] = None,
    ) -> None:
        self.__processor = processor
        self.__default_topic_spec = default_topic_spec
        self.__replacement_topic_spec = replacement_topic_spec
        self.__commit_log_topic_spec = commit_log_topic_spec
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
        if self.__replacement_topic_spec is not None:
            ret.append(self.__replacement_topic_spec)
        if self.__commit_log_topic_spec is not None:
            ret.append(self.__commit_log_topic_spec)
        return ret


def build_kafka_topic_spec_from_settings(topic_name: str) -> KafkaTopicSpec:
    return KafkaTopicSpec(
        topic_name=topic_name,
        partitions_number=settings.TOPIC_PARTITION_COUNTS.get(topic_name, 1),
    )


def build_kafka_stream_loader_from_settings(
    storage_key: StorageKey,
    processor: MessageProcessor,
    default_topic_name: str,
    pre_filter: Optional[StreamMessageFilter[KafkaPayload]] = None,
    replacement_topic_name: Optional[str] = None,
    commit_log_topic_name: Optional[str] = None,
) -> KafkaStreamLoader:
    storage_topics = {**settings.STORAGE_TOPICS.get(storage_key.value, {})}

    default_topic_spec = build_kafka_topic_spec_from_settings(
        storage_topics.pop("default", default_topic_name)
    )

    replacement_topic_spec: Optional[KafkaTopicSpec]
    if replacement_topic_name is not None:
        replacement_topic_spec = build_kafka_topic_spec_from_settings(
            storage_topics.pop("replacements", replacement_topic_name)
        )
    elif "replacements" in storage_topics:
        raise ValueError(
            f"invalid topic configuration for {storage_key!r}: replacements unsupported"
        )
    else:
        replacement_topic_spec = None

    commit_log_topic_spec: Optional[KafkaTopicSpec]
    if commit_log_topic_name is not None:
        commit_log_topic_spec = build_kafka_topic_spec_from_settings(
            storage_topics.pop("commit-log", commit_log_topic_name)
        )
    elif "commit-log" in storage_topics:
        raise ValueError(
            f"invalid topic configuration for {storage_key!r}: commit log unsupported"
        )
    else:
        commit_log_topic_spec = None

    if storage_topics.keys():
        raise ValueError(
            f"invalid topic configuration for {storage_key!r}: unknown keys {[*storage_topics.keys()]!r}"
        )

    return KafkaStreamLoader(
        processor,
        default_topic_spec,
        pre_filter,
        replacement_topic_spec,
        commit_log_topic_spec,
    )


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

    def get_batch_writer(
        self,
        metrics: MetricsBackend,
        options: ClickhouseWriterOptions = None,
        table_name: Optional[str] = None,
        chunk_size: int = settings.CLICKHOUSE_HTTP_CHUNK_SIZE,
    ) -> BatchWriter[JSONRow]:
        table_name = table_name or self.__table_schema.get_table_name()

        options = self.__update_writer_options(options)

        return self.__cluster.get_batch_writer(
            table_name, metrics, options, chunk_size=chunk_size,
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
