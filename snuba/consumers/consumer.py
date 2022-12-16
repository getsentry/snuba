import itertools
import logging
import time
from collections import defaultdict
from datetime import datetime
from functools import partial
from pickle import PickleBuffer
from typing import (
    Any,
    Callable,
    Dict,
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
from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.kafka.commit import CommitCodec
from arroyo.commit import Commit as CommitLogCommit
from arroyo.processing.strategies import FilterStep
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies import ProcessingStrategy as ProcessingStep
from arroyo.processing.strategies import ProcessingStrategyFactory, TransformStep
from arroyo.processing.strategies.dead_letter_queue import (
    InvalidKafkaMessage,
    InvalidMessages,
)
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
)
from arroyo.types import BrokerValue, Commit, Message, Partition, Topic
from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentKafkaProducer
from confluent_kafka import Producer as ConfluentProducer

from snuba.clickhouse.http import JSONRow, JSONRowEncoder, ValuesRowEncoder
from snuba.consumers.strategy_factory import ConsumerStrategyFactory
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey, are_writes_identical
from snuba.datasets.table_storage import TableWriter
from snuba.processor import (
    AggregateInsertBatch,
    InsertBatch,
    MessageProcessor,
    ReplacementBatch,
)
from snuba.state import get_config
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.writer import BatchWriter

logger = logging.getLogger("snuba.consumer")

commit_codec = CommitCodec()


class CommitLogConfig(NamedTuple):
    producer: ConfluentProducer
    topic: Topic
    group_id: str


class BytesInsertBatch(NamedTuple):
    rows: Sequence[bytes]
    origin_timestamp: Optional[datetime]

    def __reduce_ex__(
        self, protocol: SupportsIndex
    ) -> Tuple[Any, Tuple[Sequence[Any], Optional[datetime]]]:
        if int(protocol) >= 5:
            return (
                type(self),
                ([PickleBuffer(row) for row in self.rows], self.origin_timestamp),
            )
        else:
            return type(self), (self.rows, self.origin_timestamp)


class InsertBatchWriter(ProcessingStep[BytesInsertBatch]):
    def __init__(self, writer: BatchWriter[JSONRow], metrics: MetricsBackend) -> None:
        self.__writer = writer
        self.__metrics = metrics

        self.__messages: MutableSequence[Message[BytesInsertBatch]] = []
        self.__closed = False

    def poll(self) -> None:
        pass

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

        max_latency: Optional[float] = None
        latency_sum = 0.0
        max_end_to_end_latency: Optional[float] = None
        end_to_end_latency_sum = 0.0
        for message in self.__messages:
            assert isinstance(message.value, BrokerValue)
            latency = write_finish - message.value.timestamp.timestamp()
            latency_sum += latency
            if max_latency is None or latency > max_latency:
                max_latency = latency
            if message.payload.origin_timestamp is not None:
                end_to_end_latency = (
                    write_finish - message.payload.origin_timestamp.timestamp()
                )
                end_to_end_latency_sum += end_to_end_latency
                if (
                    max_end_to_end_latency is None
                    or end_to_end_latency > max_end_to_end_latency
                ):
                    max_end_to_end_latency = end_to_end_latency

        if max_latency is not None:
            self.__metrics.timing("max_latency_ms", max_latency * 1000)
            self.__metrics.timing(
                "latency_ms", (latency_sum / len(self.__messages)) * 1000
            )
        if max_end_to_end_latency is not None:
            self.__metrics.timing(
                "max_end_to_end_latency_ms", max_end_to_end_latency * 1000
            )
            self.__metrics.timing(
                "end_to_end_latency_ms",
                (end_to_end_latency_sum / len(self.__messages)) * 1000,
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

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        pass


class ReplacementBatchWriter(ProcessingStep[ReplacementBatch]):
    def __init__(self, producer: ConfluentKafkaProducer, topic: Topic) -> None:
        self.__producer = producer
        self.__topic = topic

        self.__messages: MutableSequence[Message[ReplacementBatch]] = []
        self.__closed = False

    def poll(self) -> None:
        pass

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

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        args = []
        if timeout is not None:
            args.append(timeout)

        start = time.time()
        self.__producer.flush(*args)

        logger.debug(
            "Waited %0.4f seconds for %r replacements to be flushed to %r.",
            time.time() - start,
            sum(len(message.payload.values) for message in self.__messages),
            self.__producer,
        )


class ProcessedMessageBatchWriter(
    ProcessingStep[Union[None, BytesInsertBatch, ReplacementBatch]]
):
    def __init__(
        self,
        insert_batch_writer: InsertBatchWriter,
        replacement_batch_writer: Optional[ProcessingStep[ReplacementBatch]] = None,
        # If commit log config is passed, we will produce to the commit log topic
        # upon closing each batch.
        commit_log_config: Optional[CommitLogConfig] = None,
    ) -> None:
        self.__insert_batch_writer = insert_batch_writer
        self.__replacement_batch_writer = replacement_batch_writer
        self.__commit_log_config = commit_log_config
        self.__offsets_to_produce: MutableMapping[Partition, Tuple[int, datetime]] = {}

        self.__closed = False

    def poll(self) -> None:
        self.__insert_batch_writer.poll()

        if self.__replacement_batch_writer is not None:
            self.__replacement_batch_writer.poll()

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
            message.value.offset,
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
                        self.__commit_log_config.group_id, partition, offset, timestamp
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

    def terminate(self) -> None:
        self.__closed = True

        self.__insert_batch_writer.terminate()

        if self.__replacement_batch_writer is not None:
            self.__replacement_batch_writer.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()
        self.__insert_batch_writer.join(timeout)

        if self.__replacement_batch_writer is not None:
            if timeout is not None:
                timeout = max(timeout - (time.time() - start), 0)

            self.__replacement_batch_writer.join(timeout)


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


class MultistorageCollector(
    ProcessingStep[
        Sequence[Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]]
    ]
):
    def __init__(
        self,
        steps: Mapping[
            StorageKey,
            ProcessingStep[Union[None, BytesInsertBatch, ReplacementBatch]],
        ],
        # If passed, produces to the commit log after each batch is closed
        commit_log_config: Optional[CommitLogConfig],
        ignore_errors: Optional[Set[StorageKey]] = None,
    ):
        self.__steps = steps
        self.__closed = False
        self.__commit_log_config = commit_log_config
        self.__ignore_errors = ignore_errors
        self.__messages: MutableMapping[
            StorageKey,
            List[
                Message[
                    Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]
                ]
            ],
        ] = defaultdict(list)
        self.__offsets_to_produce: MutableMapping[Partition, Tuple[int, datetime]] = {}

    def poll(self) -> None:
        for step in self.__steps.values():
            step.poll()

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
                message.value.offset,
                message.value.timestamp,
            )

    def close(self) -> None:
        self.__closed = True

        for storage_key, step in self.__steps.items():
            step.close()

    def terminate(self) -> None:
        self.__closed = True

        for step in self.__steps.values():
            step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()

        for step in self.__steps.values():
            if timeout is not None:
                timeout_remaining: Optional[float] = max(
                    timeout - (time.time() - start), 0
                )
            else:
                timeout_remaining = None

            step.join(timeout_remaining)

        if self.__commit_log_config is not None:
            for partition, (offset, timestamp) in self.__offsets_to_produce.items():
                payload = commit_codec.encode(
                    CommitLogCommit(
                        self.__commit_log_config.group_id, partition, offset, timestamp
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


def __message_to_dict(
    value: BrokerValue[KafkaPayload],
) -> Dict[str, Any]:
    return {
        "message_payload_value": value.payload.value,
        "message_offset": value.offset,
        "message_partition": value.partition.index,
        "message_topic": value.partition.topic.name,
    }


def skip_kafka_message(value: BrokerValue[KafkaPayload]) -> bool:
    # expected format is "[topic:partition_index:offset,...]" eg [snuba-metrics:0:1,snuba-metrics:0:3]
    messages_to_skip = (get_config("kafka_messages_to_skip") or "[]")[1:-1].split(",")
    return (
        f"{value.partition.topic.name}:{value.partition.index}:{value.offset}"
        in messages_to_skip
    )


def __invalid_kafka_message(
    value: BrokerValue[KafkaPayload], consumer_group: str, err: Exception
) -> InvalidKafkaMessage:
    return InvalidKafkaMessage(
        payload=value.payload.value,
        timestamp=value.timestamp,
        topic=value.partition.topic.name,
        consumer_group=consumer_group,
        partition=value.partition.index,
        offset=value.offset,
        headers=value.payload.headers,
        key=value.payload.key,
        reason=f"{err.__class__.__name__}: {err}",
    )


def process_message(
    processor: MessageProcessor, consumer_group: str, message: Message[KafkaPayload]
) -> Union[None, BytesInsertBatch, ReplacementBatch]:
    assert isinstance(message.value, BrokerValue)

    if skip_kafka_message(message.value):
        logger.warning(
            f"A consumer for {message.value.partition.topic.name} skipped a message!",
            extra=__message_to_dict(message.value),
        )
        return None

    try:
        result = processor.process_message(
            rapidjson.loads(message.payload.value),
            KafkaMessageMetadata(
                message.value.offset,
                message.value.partition.index,
                message.value.timestamp,
            ),
        )
    except Exception as err:
        logger.error(err, exc_info=True)
        raise InvalidMessages(
            [__invalid_kafka_message(message.value, consumer_group, err)]
        ) from err

    if isinstance(result, InsertBatch):
        return BytesInsertBatch(
            [json_row_encoder.encode(row) for row in result.rows],
            result.origin_timestamp,
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
        )
    elif isinstance(result, InsertBatch):
        return BytesInsertBatch(
            [json_row_encoder.encode(row) for row in result.rows],
            result.origin_timestamp,
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


def process_message_multistorage_identical_storages(
    message: Message[MultistorageKafkaPayload],
) -> MultistorageProcessedMessage:
    """
    This method is similar to process_message_multistorage except for a minor difference.
    It performs an optimization where it avoids processing a message multiple times if the
    it finds that the storages on which data needs to be written are identical. This is a
    performance optimization since we remove the message processing time completely for all
    identical storages.

    It is possible that the storage keys in the message could be a mix of identical and
    non-identical storages. This method takes into account that scenario as well.

    The reason why this method has been created rather than modifying the existing
    process_message_multistorage is to avoid doing a check for every message in cases
    where there are no identical storages like metrics.
    """
    assert isinstance(message.value, BrokerValue)
    value = rapidjson.loads(message.payload.payload.value)
    metadata = KafkaMessageMetadata(
        message.value.offset, message.value.partition.index, message.value.timestamp
    )

    intermediate_results: MutableMapping[
        StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]
    ] = {}

    for index, storage_key in enumerate(message.payload.storage_keys):
        result = None
        for other_storage_key, insert_batch in intermediate_results.items():
            if are_writes_identical(storage_key, other_storage_key):
                result = insert_batch
                break

        if result is None:
            result = _process_message_multistorage_work(
                metadata=metadata,
                storage_key=storage_key,
                storage_message=value,
            )

        intermediate_results[storage_key] = result

    return list(intermediate_results.items())


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
    metrics: MetricsBackend, storage: WritableTableStorage
) -> ProcessedMessageBatchWriter:
    replacement_batch_writer: Optional[ReplacementBatchWriter]
    stream_loader = storage.get_table_writer().get_stream_loader()
    replacement_topic_spec = stream_loader.get_replacement_topic_spec()
    if replacement_topic_spec is not None:
        # XXX: The producer is flushed when closed on strategy teardown
        # after an assignment is revoked, but never explicitly closed.
        replacement_batch_writer = ReplacementBatchWriter(
            ConfluentKafkaProducer(
                build_kafka_producer_configuration(
                    replacement_topic_spec.topic,
                    override_params={
                        "partitioner": "consistent",
                        "message.max.bytes": 50000000,  # 50MB, default is 1MB
                    },
                )
            ),
            Topic(replacement_topic_spec.topic_name),
        )
    else:
        replacement_batch_writer = None

    return ProcessedMessageBatchWriter(
        InsertBatchWriter(
            storage.get_table_writer().get_batch_writer(
                metrics,
                {"load_balancing": "in_order", "insert_distributed_sync": 1},
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
) -> ProcessingStep[MultistorageProcessedMessage]:
    return MultistorageCollector(
        {
            storage.get_storage_key(): build_multistorage_batch_writer(metrics, storage)
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
        parallel_collect: bool,
        processes: Optional[int],
        input_block_size: Optional[int],
        output_block_size: Optional[int],
        metrics: MetricsBackend,
        dead_letter_policy_creator: Optional[Callable[[], DeadLetterQueuePolicy]],
        commit_log_config: Optional[CommitLogConfig] = None,
        initialize_parallel_transform: Optional[Callable[[], None]] = None,
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

        self.__storages = storages
        self.__metrics = metrics
        self.__dead_letter_policy_creator = dead_letter_policy_creator

        self.__process_message_fn = process_message_multistorage
        if any(
            are_writes_identical(storage1.get_storage_key(), storage2.get_storage_key())
            for storage1, storage2 in itertools.combinations(self.__storages, 2)
        ):
            self.__process_message_fn = process_message_multistorage_identical_storages

        self.__inner_factory = ConsumerStrategyFactory(
            prefilter=None,
            process_message=self.__process_message_fn,
            collector=partial(
                build_collector, self.__metrics, self.__storages, commit_log_config
            ),
            max_batch_size=max_batch_size,
            max_batch_time=max_batch_time,
            processes=processes,
            input_block_size=input_block_size,
            output_block_size=output_block_size,
            initialize_parallel_transform=initialize_parallel_transform,
            parallel_collect=parallel_collect,
            dead_letter_queue_policy_creator=self.__dead_letter_policy_creator,
        )

    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:
        return TransformStep(
            partial(find_destination_storages, self.__storages),
            FilterStep(
                has_destination_storages,
                self.__inner_factory.create_with_partitions(commit, partitions),
            ),
        )
