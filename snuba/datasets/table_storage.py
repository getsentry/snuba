from typing import Any, Mapping, Optional, Sequence

from arroyo.backends.kafka import KafkaPayload

from snuba import settings
from snuba.clickhouse.http import InsertStatement, JSONRow
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseWriterOptions,
    get_cluster,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.cdc.row_processors import CdcRowProcessor
from snuba.datasets.message_filters import StreamMessageFilter
from snuba.datasets.schemas.tables import WritableTableSchema, WriteFormat
from snuba.processor import MessageProcessor
from snuba.replacers.replacer_processor import ReplacerProcessor
from snuba.settings import SLICED_KAFKA_TOPIC_MAP
from snuba.snapshots import BulkLoadSource
from snuba.snapshots.loaders import BulkLoader
from snuba.snapshots.loaders.single_table import SingleTableBulkLoader
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.metrics import MetricsBackend
from snuba.utils.schemas import ReadOnly
from snuba.utils.streams.topics import Topic, get_topic_creation_config
from snuba.writer import BatchWriter


class KafkaTopicSpec:
    def __init__(self, topic: Topic) -> None:
        self.__topic = topic

    def __hash__(self) -> int:
        return hash(self.__topic)

    @property
    def topic(self) -> Topic:
        return self.__topic

    @property
    def topic_name(self) -> str:
        """
        Returns the physical topic name for the dataset. Ignores slicing.
        Use ```get_physical_topic_name``` instead as it is slice aware.
        """
        topic = self.__topic.value
        return settings.KAFKA_TOPIC_MAP.get(topic, topic)

    def get_physical_topic_name(self, slice_id: Optional[int] = None) -> str:
        """
        Slice aware version of``topic_name``.
        """
        if slice_id is not None:
            physical_topic = SLICED_KAFKA_TOPIC_MAP[(self.topic_name, slice_id)]
        else:
            physical_topic = self.topic_name

        return physical_topic

    @property
    def partitions_number(self) -> int:
        return settings.TOPIC_PARTITION_COUNTS.get(self.__topic.value, 1)

    @property
    def replication_factor(self) -> int:
        return 1

    @property
    def topic_creation_config(self) -> Mapping[str, str]:
        return get_topic_creation_config(self.__topic)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, KafkaTopicSpec) and self.topic == other.topic


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
        subscription_scheduler_mode: Optional[SchedulingWatermarkMode] = None,
        subscription_synchronization_timestamp: Optional[str] = None,
        subscription_scheduled_topic_spec: Optional[KafkaTopicSpec] = None,
        subscription_result_topic_spec: Optional[KafkaTopicSpec] = None,
        dlq_topic_spec: Optional[KafkaTopicSpec] = None,
    ) -> None:
        assert (
            (subscription_scheduler_mode is None)
            == (subscription_scheduled_topic_spec is None)
            == (subscription_result_topic_spec is None)
        )

        self.__processor = processor
        self.__default_topic_spec = default_topic_spec
        self.__replacement_topic_spec = replacement_topic_spec
        self.__commit_log_topic_spec = commit_log_topic_spec
        self.__subscription_scheduler_mode = subscription_scheduler_mode
        self.__subscription_scheduled_topic_spec = subscription_scheduled_topic_spec
        self.__subscription_result_topic_spec = subscription_result_topic_spec
        self.__pre_filter = pre_filter
        self.__dlq_topic_spec = dlq_topic_spec

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

    def get_subscription_scheduler_mode(self) -> Optional[SchedulingWatermarkMode]:
        return self.__subscription_scheduler_mode

    def get_subscription_scheduled_topic_spec(self) -> Optional[KafkaTopicSpec]:
        return self.__subscription_scheduled_topic_spec

    def get_subscription_result_topic_spec(self) -> Optional[KafkaTopicSpec]:
        return self.__subscription_result_topic_spec

    def get_dlq_topic_spec(self) -> Optional[KafkaTopicSpec]:
        return self.__dlq_topic_spec


def build_kafka_stream_loader_from_settings(
    processor: MessageProcessor,
    default_topic: Topic,
    pre_filter: Optional[StreamMessageFilter[KafkaPayload]] = None,
    replacement_topic: Optional[Topic] = None,
    commit_log_topic: Optional[Topic] = None,
    subscription_scheduler_mode: Optional[SchedulingWatermarkMode] = None,
    subscription_scheduled_topic: Optional[Topic] = None,
    subscription_result_topic: Optional[Topic] = None,
    subscription_synchronization_timestamp: Optional[str] = None,
    dlq_topic: Optional[Topic] = None,
) -> KafkaStreamLoader:
    default_topic_spec = KafkaTopicSpec(default_topic)

    replacement_topic_spec: Optional[KafkaTopicSpec]

    if replacement_topic is not None:
        replacement_topic_spec = KafkaTopicSpec(replacement_topic)
    else:
        replacement_topic_spec = None

    commit_log_topic_spec: Optional[KafkaTopicSpec]
    if commit_log_topic is not None:
        commit_log_topic_spec = KafkaTopicSpec(commit_log_topic)
    else:
        commit_log_topic_spec = None

    subscription_scheduled_topic_spec: Optional[KafkaTopicSpec]
    if subscription_scheduled_topic is not None:
        subscription_scheduled_topic_spec = KafkaTopicSpec(subscription_scheduled_topic)
    else:
        subscription_scheduled_topic_spec = None

    subscription_result_topic_spec: Optional[KafkaTopicSpec]
    if subscription_result_topic is not None:
        subscription_result_topic_spec = KafkaTopicSpec(subscription_result_topic)
    else:
        subscription_result_topic_spec = None

    if dlq_topic is not None:
        dlq_topic_spec = KafkaTopicSpec(dlq_topic)
    else:
        dlq_topic_spec = None

    return KafkaStreamLoader(
        processor,
        default_topic_spec,
        pre_filter,
        replacement_topic_spec,
        commit_log_topic_spec,
        subscription_scheduler_mode=subscription_scheduler_mode,
        subscription_synchronization_timestamp=subscription_synchronization_timestamp,
        subscription_scheduled_topic_spec=subscription_scheduled_topic_spec,
        subscription_result_topic_spec=subscription_result_topic_spec,
        dlq_topic_spec=dlq_topic_spec,
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
        storage_set: StorageSetKey,
        write_schema: WritableTableSchema,
        stream_loader: KafkaStreamLoader,
        replacer_processor: Optional[ReplacerProcessor[Any]] = None,
        writer_options: ClickhouseWriterOptions = None,
        write_format: WriteFormat = WriteFormat.JSON,
    ) -> None:
        self.__storage_set = storage_set
        self.__table_schema = write_schema
        self.__stream_loader = stream_loader
        self.__replacer_processor = replacer_processor
        self.__writer_options = writer_options
        self.__write_format = write_format

    def get_schema(self) -> WritableTableSchema:
        return self.__table_schema

    def get_batch_writer(
        self,
        metrics: MetricsBackend,
        options: ClickhouseWriterOptions = None,
        table_name: Optional[str] = None,
        chunk_size: int = settings.CLICKHOUSE_HTTP_CHUNK_SIZE,
        slice_id: Optional[int] = None,
    ) -> BatchWriter[JSONRow]:
        table_name = table_name or self.__table_schema.get_table_name()
        if self.__write_format == WriteFormat.JSON:
            insert_statement = InsertStatement(table_name).with_format("JSONEachRow")
        elif self.__write_format == WriteFormat.VALUES:
            column_names = self.get_writeable_columns()
            insert_statement = (
                InsertStatement(table_name)
                .with_format("VALUES")
                .with_columns(column_names)
            )
        else:
            raise TypeError("unknown table format", self.__write_format)
        options = self.__update_writer_options(options)

        return get_cluster(self.__storage_set, slice_id).get_batch_writer(
            metrics,
            insert_statement,
            encoding=None,
            options=options,
            chunk_size=chunk_size,
            buffer_size=0,
        )

    def get_writeable_columns(self) -> Sequence[str]:
        return [
            column.flattened
            for column in self.get_schema().get_columns()
            if not column.type.has_modifier(ReadOnly)
        ]

    def get_bulk_writer(
        self,
        metrics: MetricsBackend,
        encoding: Optional[str],
        column_names: Sequence[str],
        options: ClickhouseWriterOptions = None,
        table_name: Optional[str] = None,
    ) -> BatchWriter[bytes]:
        table_name = table_name or self.__table_schema.get_table_name()

        options = self.__update_writer_options(options)

        return get_cluster(self.__storage_set).get_batch_writer(
            metrics,
            InsertStatement(table_name)
            .with_columns(column_names)
            .with_format("CSVWithNames"),
            encoding=encoding,
            options=options,
            chunk_size=1,
            buffer_size=settings.HTTP_WRITER_BUFFER_SIZE,
        )

    def get_bulk_loader(
        self,
        source: BulkLoadSource,
        source_table: str,
        row_processor: CdcRowProcessor,
        table_name: Optional[str] = None,
    ) -> BulkLoader:
        """
        Returns the instance of the bulk loader to populate the dataset from an
        external source when present.
        """
        table_name = table_name or self.__table_schema.get_table_name()
        return SingleTableBulkLoader(
            source=source,
            source_table=source_table,
            dest_table=table_name,
            row_processor=row_processor,
            clickhouse=get_cluster(self.__storage_set).get_query_connection(
                ClickhouseClientSettings.QUERY
            ),
        )

    def get_stream_loader(self) -> KafkaStreamLoader:
        return self.__stream_loader

    def get_replacer_processor(self) -> Optional[ReplacerProcessor[Any]]:
        """
        Returns a replacement processor if this table writer knows how to do
        replacements on the table it manages.
        """
        return self.__replacer_processor

    def __update_writer_options(
        self,
        options: ClickhouseWriterOptions = None,
    ) -> Mapping[str, Any]:
        if options is None:
            options = {}
        if self.__writer_options:
            return {**options, **self.__writer_options}
        return options
