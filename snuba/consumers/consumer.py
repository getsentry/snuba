import itertools
import logging
import random
import time
from collections import defaultdict
from datetime import datetime
from functools import partial
from pickle import PickleBuffer
from typing import (
    Any,
    Callable,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    SupportsIndex,
    Tuple,
    Union,
    cast,
)

import rapidjson
import sentry_sdk
from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.kafka.commit import CommitCodec
from arroyo.commit import Commit as CommitLogCommit
from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies import (
    CommitOffsets,
    FilterStep,
    ProcessingStrategy,
    ProcessingStrategyFactory,
    Reduce,
    RunTask,
    RunTaskInThreads,
    RunTaskWithMultiprocessing,
)
from arroyo.types import (
    BaseValue,
    BrokerValue,
    Commit,
    FilteredPayload,
    Message,
    Partition,
    Topic,
)
from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentKafkaProducer
from confluent_kafka import Producer as ConfluentProducer

from snuba import environment, state
from snuba.clickhouse.http import JSONRow, JSONRowEncoder, ValuesRowEncoder
from snuba.consumers.schemas import _NOOP_CODEC, get_json_codec
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import TableWriter
from snuba.processor import (
    AggregateInsertBatch,
    InsertBatch,
    MessageProcessor,
    ReplacementBatch,
)
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic as SnubaTopic
from snuba.writer import BatchWriter

metrics = MetricsWrapper(environment.metrics, "consumer")

logger = logging.getLogger("snuba.consumer")

commit_codec = CommitCodec()

_LAST_INVALID_MESSAGE: MutableMapping[str, float] = {}
LOG_INVALID_MESSAGE_FREQ_SEC = 1


class CommitLogConfig(NamedTuple):
    producer: ConfluentProducer
    topic: Topic
    group_id: str


class BytesInsertBatch(NamedTuple):
    rows: Sequence[bytes]
    origin_timestamp: Optional[datetime]
    sentry_received_timestamp: Optional[datetime] = None

    def __reduce_ex__(
        self, protocol: SupportsIndex
    ) -> Tuple[Any, Tuple[Sequence[Any], Optional[datetime], Optional[datetime]]]:
        if int(protocol) >= 5:
            return (
                type(self),
                (
                    [PickleBuffer(row) for row in self.rows],
                    self.origin_timestamp,
                    self.sentry_received_timestamp,
                ),
            )
        else:
            return type(self), (
                self.rows,
                self.origin_timestamp,
                self.sentry_received_timestamp,
            )


class LatencyRecorder:
    def __init__(self) -> None:
        self._sum = 0.0
        self._max: Optional[float] = None
        self._msg_count = 0

    def record(self, latency_seconds: float) -> None:
        self._msg_count += 1
        self._sum += latency_seconds
        if self._max is None or latency_seconds > self._max:
            self._max = latency_seconds

    @property
    def avg_ms(self) -> float:
        return (self._sum / self._msg_count) * 1000

    @property
    def max_ms(self) -> Optional[float]:
        if not self._max:
            return None
        return self._max * 1000


class InsertBatchWriter:
    def __init__(self, writer: BatchWriter[JSONRow], metrics: MetricsBackend) -> None:
        self.__writer = writer
        self.__metrics = metrics

        self.__messages: MutableSequence[Message[BytesInsertBatch]] = []
        self.__closed = False

    def submit(self, message: Message[BytesInsertBatch]) -> None:
        assert not self.__closed

        self.__messages.append(message)

    def close(self) -> None:
        self.__closed = True

        if not self.__messages:
            return

        write_start = time.time()
        self.__writer.write(
            itertools.chain.from_iterable(
                message.payload.rows for message in self.__messages
            )
        )
        write_finish = time.time()

        snuba_latency_recorder = LatencyRecorder()
        end_to_end_latency_recorder = LatencyRecorder()
        sentry_received_latency_recorder = LatencyRecorder()

        for message in self.__messages:
            assert isinstance(message.value, BrokerValue)

            latency = write_finish - message.value.timestamp.timestamp()
            snuba_latency_recorder.record(latency)

            origin_timestamp = message.payload.origin_timestamp
            if origin_timestamp is not None:
                end_to_end_latency = write_finish - origin_timestamp.timestamp()
                end_to_end_latency_recorder.record(end_to_end_latency)

            sentry_received_timestamp = message.payload.sentry_received_timestamp
            if sentry_received_timestamp is not None:
                sentry_received_latency = (
                    write_finish - sentry_received_timestamp.timestamp()
                )
                sentry_received_latency_recorder.record(sentry_received_latency)

        if snuba_latency_recorder.max_ms:
            self.__metrics.timing("max_latency_ms", snuba_latency_recorder.max_ms)
            self.__metrics.timing("latency_ms", snuba_latency_recorder.avg_ms)
        if end_to_end_latency_recorder.max_ms:
            self.__metrics.timing(
                "max_end_to_end_latency_ms", end_to_end_latency_recorder.max_ms
            )
            self.__metrics.timing(
                "end_to_end_latency_ms",
                end_to_end_latency_recorder.avg_ms,
            )

        if sentry_received_latency_recorder.max_ms:
            self.__metrics.timing(
                "max_sentry_received_latency_ms",
                sentry_received_latency_recorder.max_ms,
            )
            self.__metrics.timing(
                "sentry_received_latency_ms",
                sentry_received_latency_recorder.avg_ms,
            )

        self.__metrics.timing("batch_write_ms", (write_finish - write_start) * 1000)
        rows = sum(len(message.payload.rows) for message in self.__messages)
        self.__metrics.increment("batch_write_msgs", rows)

        logger.debug(
            "Waited %0.4f seconds for %r rows to be written to %r.",
            write_finish - write_start,
            rows,
            self.__writer,
        )


class ReplacementBatchWriter:
    def __init__(self, producer: ConfluentKafkaProducer, topic: Topic) -> None:
        self.__producer = producer
        self.__topic = topic

        self.__messages: MutableSequence[Message[ReplacementBatch]] = []
        self.__closed = False

    def submit(self, message: Message[ReplacementBatch]) -> None:
        assert not self.__closed

        self.__messages.append(message)

    def __delivery_callback(
        self, error: Optional[Exception], message: Message[ReplacementBatch]
    ) -> None:
        if error is not None:
            # errors are KafkaError objects and inherit from BaseException
            raise error

    def close(self) -> None:
        self.__closed = True

        if not self.__messages:
            return

        for message in self.__messages:
            batch = message.payload
            key = batch.key.encode("utf-8")
            for value in batch.values:
                self.__producer.produce(
                    self.__topic.name,
                    key=key,
                    value=rapidjson.dumps(value).encode("utf-8"),
                    on_delivery=self.__delivery_callback,
                )

        self.__producer.flush()


class ProcessedMessageBatchWriter:
    def __init__(
        self,
        insert_batch_writer: InsertBatchWriter,
        replacement_batch_writer: Optional[ReplacementBatchWriter] = None,
        # If commit log config is passed, we will produce to the commit log topic
        # upon closing each batch.
        commit_log_config: Optional[CommitLogConfig] = None,
    ) -> None:
        self.__insert_batch_writer = insert_batch_writer
        self.__replacement_batch_writer = replacement_batch_writer
        self.__commit_log_config = commit_log_config
        self.__offsets_to_produce: MutableMapping[Partition, Tuple[int, datetime]] = {}

        self.__closed = False

    def submit(
        self, message: Message[Union[None, BytesInsertBatch, ReplacementBatch]]
    ) -> None:
        assert not self.__closed

        if message.payload is None:
            return

        if isinstance(message.payload, BytesInsertBatch):
            self.__insert_batch_writer.submit(cast(Message[BytesInsertBatch], message))
        elif isinstance(message.payload, ReplacementBatch):
            if self.__replacement_batch_writer is None:
                raise TypeError("writer not configured to support replacements")

            self.__replacement_batch_writer.submit(
                cast(Message[ReplacementBatch], message)
            )
        else:
            raise TypeError("unexpected payload type")

        assert isinstance(message.value, BrokerValue)
        self.__offsets_to_produce[message.value.partition] = (
            message.value.next_offset,
            message.value.timestamp,
        )

    def __commit_message_delivery_callback(
        self, error: Optional[KafkaError], message: ConfluentMessage
    ) -> None:
        if error is not None:
            raise Exception(error.str())

    def close(self) -> None:
        self.__closed = True

        self.__insert_batch_writer.close()

        if self.__replacement_batch_writer is not None:
            self.__replacement_batch_writer.close()

        if self.__commit_log_config is not None:
            for partition, (offset, timestamp) in self.__offsets_to_produce.items():
                payload = commit_codec.encode(
                    CommitLogCommit(
                        self.__commit_log_config.group_id,
                        partition,
                        offset,
                        datetime.timestamp(timestamp),
                        None,
                    )
                )
                self.__commit_log_config.producer.produce(
                    self.__commit_log_config.topic.name,
                    key=payload.key,
                    value=payload.value,
                    headers=payload.headers,
                    on_delivery=self.__commit_message_delivery_callback,
                )
                self.__commit_log_config.producer.poll(0.0)
        self.__offsets_to_produce.clear()


json_row_encoder = JSONRowEncoder()

values_row_encoders: MutableMapping[StorageKey, ValuesRowEncoder] = dict()


def get_values_row_encoder(storage_key: StorageKey) -> ValuesRowEncoder:
    from snuba.datasets.storages.factory import get_writable_storage

    if storage_key not in values_row_encoders:
        table_writer = get_writable_storage(storage_key).get_table_writer()
        values_row_encoders[storage_key] = ValuesRowEncoder(
            table_writer.get_writeable_columns()
        )

    return values_row_encoders[storage_key]


def build_batch_writer(
    table_writer: TableWriter,
    metrics: MetricsBackend,
    replacements_producer: Optional[ConfluentKafkaProducer] = None,
    replacements_topic: Optional[Topic] = None,
    commit_log_config: Optional[CommitLogConfig] = None,
    slice_id: Optional[int] = None,
) -> Callable[[], ProcessedMessageBatchWriter]:

    assert not (replacements_producer is None) ^ (replacements_topic is None)
    supports_replacements = replacements_producer is not None

    writer = table_writer.get_batch_writer(
        metrics,
        {"load_balancing": "in_order", "insert_distributed_sync": 1},
        slice_id=slice_id,
    )

    def build_writer() -> ProcessedMessageBatchWriter:
        insert_batch_writer = InsertBatchWriter(
            writer, MetricsWrapper(metrics, "insertions")
        )

        replacement_batch_writer: Optional[ReplacementBatchWriter]
        if supports_replacements:
            assert replacements_producer is not None
            assert replacements_topic is not None
            replacement_batch_writer = ReplacementBatchWriter(
                replacements_producer, replacements_topic
            )
        else:
            replacement_batch_writer = None

        return ProcessedMessageBatchWriter(
            insert_batch_writer, replacement_batch_writer, commit_log_config
        )

    return build_writer


class MultistorageCollector:
    def __init__(
        self,
        steps: Mapping[StorageKey, ProcessedMessageBatchWriter],
        # If passed, produces to the commit log after each batch is closed
        commit_log_config: Optional[CommitLogConfig],
        ignore_errors: Optional[Set[StorageKey]] = None,
    ):
        self.__steps = steps
        self.__closed = False
        self.__commit_log_config = commit_log_config
        self.__messages: MutableMapping[
            StorageKey,
            List[
                Message[
                    Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]
                ]
            ],
        ] = defaultdict(list)
        self.__offsets_to_produce: MutableMapping[Partition, Tuple[int, datetime]] = {}

    def submit(
        self,
        message: Message[
            Sequence[Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]]
        ],
    ) -> None:
        assert not self.__closed

        for storage_key, payload in message.payload:
            writer_message = message.replace(payload)
            self.__steps[storage_key].submit(writer_message)

            # we collect the messages in self.__messages in the off chance
            # that we get an error submitting a batch and need to forward
            # these message to the dead letter topic. The payload doesn't
            # have storage information so we need to keep the storage_key
            other_message = message.replace((storage_key, payload))

            self.__messages[storage_key].append(other_message)
            assert isinstance(message.value, BrokerValue)
            self.__offsets_to_produce[message.value.partition] = (
                message.value.next_offset,
                message.value.timestamp,
            )

    def close(self) -> None:
        self.__closed = True

        for storage_key, step in self.__steps.items():
            step.close()

        if self.__commit_log_config is not None:
            for partition, (offset, timestamp) in self.__offsets_to_produce.items():
                payload = commit_codec.encode(
                    CommitLogCommit(
                        self.__commit_log_config.group_id,
                        partition,
                        offset,
                        datetime.timestamp(timestamp),
                        None,
                    )
                )
                self.__commit_log_config.producer.produce(
                    self.__commit_log_config.topic.name,
                    key=payload.key,
                    value=payload.value,
                    headers=payload.headers,
                    on_delivery=self.__commit_message_delivery_callback,
                )
                self.__commit_log_config.producer.poll(0.0)

            self.__commit_log_config.producer.flush()

        self.__messages = {}
        self.__offsets_to_produce.clear()

    def __commit_message_delivery_callback(
        self, error: Optional[KafkaError], message: ConfluentMessage
    ) -> None:
        if error is not None:
            raise Exception(error.str())


class MultistorageKafkaPayload(NamedTuple):
    storage_keys: Sequence[StorageKey]
    payload: KafkaPayload


MultistorageProcessedMessage = Sequence[
    Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]
]


def process_message(
    processor: MessageProcessor,
    consumer_group: str,
    snuba_logical_topic: SnubaTopic,
    enforce_schema: bool,
    message: Message[KafkaPayload],
) -> Union[None, BytesInsertBatch, ReplacementBatch]:
    local_metrics = MetricsWrapper(
        metrics,
        tags={
            "consumer_group": consumer_group,
            "snuba_logical_topic": snuba_logical_topic.name,
        },
    )

    validate_sample_rate = (
        state.get_float_config(f"validate_schema_{snuba_logical_topic.name}", 1.0)
        or 0.0
    )

    assert isinstance(message.value, BrokerValue)
    try:
        codec = get_json_codec(snuba_logical_topic)
        should_validate = random.random() < validate_sample_rate
        start = time.time()

        decoded = codec.decode(message.payload.value, validate=False)

        if should_validate:
            with sentry_sdk.push_scope() as scope:
                scope.add_attachment(
                    bytes=message.payload.value, filename="message.txt"
                )
                scope.set_tag("snuba_logical_topic", snuba_logical_topic.name)

                try:
                    # Occasionally log errors if no validator is configured
                    if codec == _NOOP_CODEC:
                        raise Exception("No validator configured for topic")

                    codec.validate(decoded)
                except Exception as err:
                    local_metrics.increment(
                        "schema_validation.failed",
                    )

                    if (
                        _LAST_INVALID_MESSAGE.get(snuba_logical_topic.name, 0)
                        < start - LOG_INVALID_MESSAGE_FREQ_SEC
                    ):
                        _LAST_INVALID_MESSAGE[snuba_logical_topic.name] = start
                        sentry_sdk.set_tag("invalid_message_schema", "true")
                        logger.warning(err, exc_info=True)
                    if enforce_schema:
                        raise Exception(
                            f"Validation Error - {snuba_logical_topic.value}"
                        ) from err

            # TODO: this is not the most efficient place to emit a metric, but
            # as long as should_validate is behind a sample rate it should be
            # OK.
            local_metrics.timing(
                "codec_decode_and_validate",
                (time.time() - start) * 1000,
            )

        result = processor.process_message(
            decoded,
            KafkaMessageMetadata(
                message.value.offset,
                message.value.partition.index,
                message.value.timestamp,
            ),
        )
    except Exception as err:
        local_metrics.increment(
            "invalid_message",
            tags={
                "topic": snuba_logical_topic.name,
                "processor": type(processor).__name__,
            },
        )
        with sentry_sdk.push_scope() as scope:
            scope.set_tag("invalid_message", "true")
            logger.warning(err, exc_info=True)
            value = message.value
            raise InvalidMessage(value.partition, value.offset) from err

    if isinstance(result, InsertBatch):
        return BytesInsertBatch(
            [json_row_encoder.encode(row) for row in result.rows],
            result.origin_timestamp,
            result.sentry_received_timestamp,
        )
    else:
        return result


def _process_message_multistorage_work(
    metadata: KafkaMessageMetadata, storage_key: StorageKey, storage_message: Any
) -> Union[None, BytesInsertBatch, ReplacementBatch]:
    result = (
        get_writable_storage(storage_key)
        .get_table_writer()
        .get_stream_loader()
        .get_processor()
        .process_message(storage_message, metadata)
    )

    if isinstance(result, AggregateInsertBatch):
        values_row_encoder = get_values_row_encoder(storage_key)
        return BytesInsertBatch(
            [values_row_encoder.encode(row) for row in result.rows],
            result.origin_timestamp,
            result.sentry_received_timestamp,
        )
    elif isinstance(result, InsertBatch):
        return BytesInsertBatch(
            [json_row_encoder.encode(row) for row in result.rows],
            result.origin_timestamp,
            result.sentry_received_timestamp,
        )
    else:
        return result


def process_message_multistorage(
    message: Message[MultistorageKafkaPayload],
) -> MultistorageProcessedMessage:
    assert isinstance(message.value, BrokerValue)
    value = rapidjson.loads(message.payload.payload.value)
    metadata = KafkaMessageMetadata(
        message.value.offset, message.value.partition.index, message.value.timestamp
    )

    results: MutableSequence[
        Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]
    ] = []

    for index, storage_key in enumerate(message.payload.storage_keys):
        result = _process_message_multistorage_work(
            metadata=metadata, storage_key=storage_key, storage_message=value
        )
        results.append((storage_key, result))

    return results


def has_destination_storages(message: Message[MultistorageKafkaPayload]) -> bool:
    return len(message.payload.storage_keys) > 0


def find_destination_storages(
    storages: Sequence[WritableTableStorage], message: Message[KafkaPayload]
) -> MultistorageKafkaPayload:
    storage_keys: MutableSequence[StorageKey] = []
    for storage in storages:
        filter = storage.get_table_writer().get_stream_loader().get_pre_filter()
        if filter is None or not filter.should_drop(message):
            storage_keys.append(storage.get_storage_key())
    return MultistorageKafkaPayload(storage_keys, message.payload)


def build_multistorage_batch_writer(
    metrics: MetricsBackend,
    storage: WritableTableStorage,
    replacements: Optional[Topic],
    slice_id: Optional[int],
) -> ProcessedMessageBatchWriter:
    replacement_batch_writer: Optional[ReplacementBatchWriter]
    stream_loader = storage.get_table_writer().get_stream_loader()
    replacement_topic_spec = stream_loader.get_replacement_topic_spec()
    if replacements is not None:
        assert replacement_topic_spec is not None
        # XXX: The producer is flushed when closed on strategy teardown
        # after an assignment is revoked, but never explicitly closed.
        replacement_batch_writer = ReplacementBatchWriter(
            ConfluentKafkaProducer(
                build_kafka_producer_configuration(
                    replacement_topic_spec.topic,
                    override_params={
                        "partitioner": "consistent",
                        "message.max.bytes": 10000000,  # 10MB, default is 1MB
                    },
                )
            ),
            replacements,
        )
    else:
        replacement_batch_writer = None

    return ProcessedMessageBatchWriter(
        InsertBatchWriter(
            storage.get_table_writer().get_batch_writer(
                metrics,
                {"load_balancing": "in_order", "insert_distributed_sync": 1},
                slice_id=slice_id,
            ),
            MetricsWrapper(
                metrics,
                "insertions",
                {"storage": storage.get_storage_key().value},
            ),
        ),
        replacement_batch_writer,
    )


def build_collector(
    metrics: MetricsBackend,
    storages: Sequence[WritableTableStorage],
    commit_log_config: Optional[CommitLogConfig],
    replacements: Optional[Topic],
    slice_id: Optional[int],
) -> MultistorageCollector:
    return MultistorageCollector(
        {
            storage.get_storage_key(): build_multistorage_batch_writer(
                metrics, storage, replacements, slice_id
            )
            for storage in storages
        },
        commit_log_config,
        ignore_errors={
            storage.get_storage_key()
            for storage in storages
            if storage.get_is_write_error_ignorable() is True
        },
    )


class MultistorageConsumerProcessingStrategyFactory(
    ProcessingStrategyFactory[KafkaPayload]
):
    def __init__(
        self,
        storages: Sequence[WritableTableStorage],
        max_batch_size: int,
        max_batch_time: float,
        processes: Optional[int],
        input_block_size: Optional[int],
        output_block_size: Optional[int],
        metrics: MetricsBackend,
        slice_id: Optional[int],
        commit_log_config: Optional[CommitLogConfig] = None,
        replacements: Optional[Topic] = None,
        initialize_parallel_transform: Optional[Callable[[], None]] = None,
        parallel_collect_timeout: float = 10.0,
    ) -> None:
        if processes is not None:
            assert input_block_size is not None, "input block size required"
            assert output_block_size is not None, "output block size required"
        else:
            assert (
                input_block_size is None
            ), "input block size cannot be used without processes"
            assert (
                output_block_size is None
            ), "output block size cannot be used without processes"

        self.__input_block_size = input_block_size
        self.__output_block_size = output_block_size
        self.__initialize_parallel_transform = initialize_parallel_transform

        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time
        self.__processes = processes

        self.__storages = storages
        self.__metrics = metrics

        self.__process_message_fn = process_message_multistorage

        self.__collector = partial(
            build_collector,
            self.__metrics,
            self.__storages,
            commit_log_config,
            replacements,
            slice_id,
        )

    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:
        def accumulator(
            batch_writer: MultistorageCollector,
            message: BaseValue[MultistorageProcessedMessage],
        ) -> MultistorageCollector:
            batch_writer.submit(Message(message))
            return batch_writer

        def flush_batch(
            message: Message[MultistorageCollector],
        ) -> Message[MultistorageCollector]:
            message.payload.close()
            return message

        collect = Reduce[MultistorageProcessedMessage, MultistorageCollector](
            self.__max_batch_size,
            self.__max_batch_time,
            accumulator,
            self.__collector,
            RunTaskInThreads(flush_batch, 1, 1, CommitOffsets(commit)),
        )

        transform_function = self.__process_message_fn

        inner_strategy: ProcessingStrategy[
            Union[FilteredPayload, MultistorageKafkaPayload]
        ]

        if self.__processes is None:
            inner_strategy = RunTask(transform_function, collect)
        else:
            assert self.__input_block_size is not None
            assert self.__output_block_size is not None
            inner_strategy = RunTaskWithMultiprocessing(
                transform_function,
                collect,
                self.__processes,
                max_batch_size=self.__max_batch_size,
                max_batch_time=self.__max_batch_time,
                input_block_size=self.__input_block_size,
                output_block_size=self.__output_block_size,
                initializer=self.__initialize_parallel_transform,
            )

        return RunTask(
            partial(find_destination_storages, self.__storages),
            FilterStep(
                has_destination_storages,
                inner_strategy,
            ),
        )
