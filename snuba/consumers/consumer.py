import copy
import itertools
import logging
import time
from collections import defaultdict, deque
from concurrent.futures import Future
from datetime import datetime
from pickle import PickleBuffer
from typing import (
    Any,
    Callable,
    Deque,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

import rapidjson
from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Producer as AbstractProducer
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies import ProcessingStrategy as ProcessingStep
from arroyo.processing.strategies import ProcessingStrategyFactory
from arroyo.processing.strategies.streaming import (
    CollectStep,
    FilterStep,
    ParallelTransformStep,
    TransformStep,
)
from arroyo.types import Position
from confluent_kafka import Producer as ConfluentKafkaProducer

from snuba.clickhouse.http import JSONRow, JSONRowEncoder, ValuesRowEncoder
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey, are_writes_identical
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.table_storage import TableWriter
from snuba.environment import setup_sentry
from snuba.processor import (
    AggregateInsertBatch,
    InsertBatch,
    MessageProcessor,
    ReplacementBatch,
)
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.writer import BatchWriter, MockBatchWriter

logger = logging.getLogger("snuba.consumer")


class BytesInsertBatch(NamedTuple):
    rows: Sequence[bytes]
    origin_timestamp: Optional[datetime]

    def __reduce_ex__(
        self, protocol: int
    ) -> Tuple[Any, Tuple[Sequence[Any], Optional[datetime]]]:
        if protocol >= 5:
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

        for message in self.__messages:
            self.__metrics.timing(
                "latency_ms", (write_finish - message.timestamp.timestamp()) * 1000
            )
            if message.payload.origin_timestamp is not None:
                self.__metrics.timing(
                    "end_to_end_latency_ms",
                    (write_finish - message.payload.origin_timestamp.timestamp())
                    * 1000,
                )
        self.__metrics.timing("batch_write_ms", write_finish - write_start)
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


class MockReplacementBatchWriter(ProcessingStep[ReplacementBatch]):
    """
    Fake replacement writer used to run consumer load tests.
    Contrarily to the mock writer. THis one does not introduce a delay.
    This is because replacement are rare enough that adding the
    delay would not change the result of the test.
    """

    def poll(self) -> None:
        pass

    def submit(self, message: Message[ReplacementBatch]) -> None:
        pass

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        pass


class ProcessedMessageBatchWriter(
    ProcessingStep[Union[None, BytesInsertBatch, ReplacementBatch]]
):
    def __init__(
        self,
        insert_batch_writer: InsertBatchWriter,
        replacement_batch_writer: Optional[ProcessingStep[ReplacementBatch]] = None,
    ) -> None:
        self.__insert_batch_writer = insert_batch_writer
        self.__replacement_batch_writer = replacement_batch_writer

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

    def close(self) -> None:
        self.__closed = True

        self.__insert_batch_writer.close()

        if self.__replacement_batch_writer is not None:
            self.__replacement_batch_writer.close()

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
) -> Callable[[], ProcessedMessageBatchWriter]:

    assert not (replacements_producer is None) ^ (replacements_topic is None)
    supports_replacements = replacements_producer is not None

    writer = table_writer.get_batch_writer(
        metrics, {"load_balancing": "in_order", "insert_distributed_sync": 1},
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
            insert_batch_writer, replacement_batch_writer
        )

    return build_writer


def build_mock_batch_writer(
    storage: WritableTableStorage,
    with_replacements: bool,
    metrics: MetricsBackend,
    avg_write_latency: int,
    std_deviation: int,
) -> Callable[[], ProcessedMessageBatchWriter]:
    def build_writer() -> ProcessedMessageBatchWriter:
        return ProcessedMessageBatchWriter(
            InsertBatchWriter(
                MockBatchWriter(
                    storage.get_storage_key(), avg_write_latency, std_deviation
                ),
                MetricsWrapper(
                    metrics,
                    "mock_insertions",
                    {"storage": storage.get_storage_key().value},
                ),
            ),
            MockReplacementBatchWriter() if with_replacements is not None else None,
        )

    return build_writer


class DeadLetterStep(
    ProcessingStep[Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]]
):
    """
    If a batch failed to be inserted to ClickHouse for some reason, we put those messages
    into a dead letter topic so that they can be consumed later and tried again.

    This is only possible for the MultiStorageConsumerFactory right now, and it is an
    optional step for the MultistorageCollector. Additionally, this is only enabled if
    the storages have `ignore_errors` set to True - which is the case for the errors_v2
    and transaction_v2 storages.

    """

    def __init__(
        self,
        producer: AbstractProducer[KafkaPayload],
        topic: Topic,
        metrics: MetricsBackend,
    ):
        self.__producer = producer
        self.__topic = topic
        self.__metrics = metrics
        self.__futures: Deque[Future[Message[KafkaPayload]]] = deque()
        self.__closed = False

    def __format_payload(
        self,
        message: Message[
            Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]
        ],
    ) -> List[KafkaPayload]:
        kafka_payloads: List[KafkaPayload] = []
        storage_key, payload = message.payload
        if isinstance(payload, BytesInsertBatch):
            for row in payload.rows:
                kafka_payloads.append(
                    KafkaPayload(storage_key.value.encode("utf-8"), row, [])
                )
        return kafka_payloads

    def poll(self) -> None:
        pass

    def submit(
        self,
        message: Message[
            Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]
        ],
    ) -> None:
        submit_start = time.time()
        payloads = self.__format_payload(message)

        produce_start = time.time()
        for payload in payloads:
            self.__futures.append(self.__producer.produce(self.__topic, payload))
        submit_end = time.time()

        submit_duration_ms = (submit_end - submit_start) * 1000.0
        produce_duration_ms = (submit_end - produce_start) * 1000.0
        self.__metrics.timing("dead_letter_step.submit_duration_ms", submit_duration_ms)
        self.__metrics.timing(
            "dead_letter_step.produce_duration_ms", produce_duration_ms
        )

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()

        while self.__futures:
            if not timeout or timeout < time.time() - start:
                self.__metrics.increment(
                    "dead_letter_step.join_error", tags={"reason": "timeout"}
                )
                break
            if self.__futures[0].done():
                future = self.__futures.popleft()
                try:
                    future.result()
                except Exception:
                    self.__metrics.increment(
                        "dead_letter_step.join_error", tags={"reason": "exception"}
                    )
                    logger.warning(
                        "Failed dead letter queue future result", exc_info=True
                    )


class MultistorageCollector(
    ProcessingStep[
        Sequence[Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]]
    ]
):
    def __init__(
        self,
        steps: Mapping[
            StorageKey, ProcessingStep[Union[None, BytesInsertBatch, ReplacementBatch]],
        ],
        dead_letter_step: Optional[
            ProcessingStep[
                Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]
            ]
        ],
        ignore_errors: Optional[Set[StorageKey]] = None,
    ):
        self.__steps = steps
        self.__closed = False
        self.__ignore_errors = ignore_errors
        self.__dead_letter_step = dead_letter_step
        self.__messages: MutableMapping[
            StorageKey,
            List[
                Message[
                    Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]
                ]
            ],
        ] = defaultdict(list)

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
            writer_message = Message(
                message.partition,
                message.offset,
                payload,
                message.timestamp,
                message.next_offset,
            )
            self.__steps[storage_key].submit(writer_message)

            # we collect the messages in self.__messages in the off chance
            # that we get an error submitting a batch and need to forward
            # these message to the dead letter topic. The payload doesn't
            # have storage information so we need to keep the storage_key
            other_message = Message(
                message.partition,
                message.offset,
                (storage_key, payload),
                message.timestamp,
                message.next_offset,
            )

            self.__messages[storage_key].append(other_message)

    def close(self) -> None:
        self.__closed = True

        for storage_key, step in self.__steps.items():
            if self.__ignore_errors and storage_key in self.__ignore_errors:
                try:
                    step.close()
                except Exception as e:
                    messages = self.__messages[storage_key]
                    if self.__dead_letter_step and messages:
                        logger.info(
                            f"Submitting {len(messages)} {storage_key.value} messages to dead letter step..."
                        )
                        for message in self.__messages[storage_key]:
                            self.__dead_letter_step.submit(message)
                    logger.warning("Error while writing data to clickhouse", exc_info=e)
            else:
                step.close()

    def terminate(self) -> None:
        self.__closed = True

        for step in self.__steps.values():
            step.terminate()

        if self.__dead_letter_step:
            self.__dead_letter_step.terminate()

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

        self.__messages = {}
        if self.__dead_letter_step:
            # timeout of 5.0 was set arbitrarily, can be changed
            self.__dead_letter_step.join(5.0)


class MultistorageKafkaPayload(NamedTuple):
    storage_keys: Sequence[StorageKey]
    payload: KafkaPayload


def process_message(
    processor: MessageProcessor, message: Message[KafkaPayload]
) -> Union[None, BytesInsertBatch, ReplacementBatch]:
    result = processor.process_message(
        rapidjson.loads(message.payload.value),
        KafkaMessageMetadata(
            message.offset, message.partition.index, message.timestamp
        ),
    )

    if isinstance(result, InsertBatch):
        return BytesInsertBatch(
            [json_row_encoder.encode(row) for row in result.rows],
            result.origin_timestamp,
        )
    else:
        return result


def process_message_multistorage_work(
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
) -> Sequence[Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]]:
    value = rapidjson.loads(message.payload.payload.value)
    metadata = KafkaMessageMetadata(
        message.offset, message.partition.index, message.timestamp
    )

    results: MutableSequence[
        Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]
    ] = []

    for index, storage_key in enumerate(message.payload.storage_keys):
        # The `process_message` api of individual storage processors could modify the content of the payload
        # in some cases. For example, they can remove redundant data from the payload to decrease size of
        # storage on clickhouse. Hence, we need to make copies of the payload and send the copies to the api.
        # The last storage will use the original payload to avoid unnecessary copying.
        storage_message = (
            copy.deepcopy(value)
            if (index < len(message.payload.storage_keys) - 1)
            else value
        )

        result = process_message_multistorage_work(
            metadata=metadata, storage_key=storage_key, storage_message=storage_message
        )
        results.append((storage_key, result))

    return results


def process_message_multistorage_identical_storages(
    message: Message[MultistorageKafkaPayload],
) -> Sequence[Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]]:
    """
    This method is similar to process_message_multistorage except for a minor difference.
    It performs an optimization where it avoids processing a message multiple times if the
    it finds that the storages on which data needs to be written are identical. This is a
    performance optimization since we remove the message processing time completely for all
    identical storages like errors and errors_v2.

    It is possible that the storage keys in the message could be a mix of identical and
    non-identical storages. This method takes into account that scenario as well.

    The reason why this method has been created rather than modifying the existing
    process_message_multistorage is to avoid doing a check for every message in cases
    where there are no identical storages like metrics.
    """
    value = rapidjson.loads(message.payload.payload.value)
    metadata = KafkaMessageMetadata(
        message.offset, message.partition.index, message.timestamp
    )

    intermediate_results: MutableMapping[
        StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]
    ] = {}

    for index, storage_key in enumerate(message.payload.storage_keys):
        skip_processing = False
        for other_storage_key, insert_batch in intermediate_results.items():
            if are_writes_identical(storage_key, other_storage_key):
                intermediate_results[storage_key] = copy.copy(insert_batch)
                skip_processing = True
                break

        if skip_processing:
            continue

        # The `process_message` api of individual storage processors could modify the content of the payload
        # in some cases. For example, they can remove redundant data from the payload to decrease size of
        # storage on clickhouse. Hence, we need to make copies of the payload and send the copies to the api.
        # The last storage will use the original payload to avoid unnecessary copying.
        storage_message = (
            copy.deepcopy(value)
            if (index < len(message.payload.storage_keys) - 1)
            else value
        )

        result = process_message_multistorage_work(
            metadata=metadata, storage_key=storage_key, storage_message=storage_message
        )
        intermediate_results[storage_key] = result

    return list(intermediate_results.items())


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
        producer: Optional[AbstractProducer[KafkaPayload]],
        topic: Optional[Topic],
    ):
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
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time
        self.__processes = processes
        self.__input_block_size = input_block_size
        self.__output_block_size = output_block_size
        self.__metrics = metrics
        self.__producer = producer
        self.__topic = topic

        self.__process_message_fn = process_message_multistorage
        for storage1, storage2 in itertools.combinations(self.__storages, 2):
            if are_writes_identical(
                storage1.get_storage_key(), storage2.get_storage_key()
            ):
                self.__process_message_fn = (
                    process_message_multistorage_identical_storages
                )
                break

    def __find_destination_storages(
        self, message: Message[KafkaPayload]
    ) -> MultistorageKafkaPayload:
        storage_keys: MutableSequence[StorageKey] = []
        for storage in self.__storages:
            filter = storage.get_table_writer().get_stream_loader().get_pre_filter()
            if filter is None or not filter.should_drop(message):
                storage_keys.append(storage.get_storage_key())
        return MultistorageKafkaPayload(storage_keys, message.payload)

    def __has_destination_storages(
        self, message: Message[MultistorageKafkaPayload]
    ) -> bool:
        return len(message.payload.storage_keys) > 0

    def __build_batch_writer(
        self, storage: WritableTableStorage
    ) -> ProcessedMessageBatchWriter:
        replacement_batch_writer: Optional[ReplacementBatchWriter]
        stream_loader = storage.get_table_writer().get_stream_loader()
        replacement_topic_spec = stream_loader.get_replacement_topic_spec()
        default_topic_spec = stream_loader.get_default_topic_spec()
        if replacement_topic_spec is not None:
            # XXX: The producer is flushed when closed on strategy teardown
            # after an assignment is revoked, but never explicitly closed.
            # XXX: This assumes that the Kafka cluster used for the input topic
            # to the storage is the same as the replacement topic.
            replacement_batch_writer = ReplacementBatchWriter(
                ConfluentKafkaProducer(
                    build_kafka_producer_configuration(
                        default_topic_spec.topic,
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
                    self.__metrics,
                    {"load_balancing": "in_order", "insert_distributed_sync": 1},
                ),
                MetricsWrapper(
                    self.__metrics,
                    "insertions",
                    {"storage": storage.get_storage_key().value},
                ),
            ),
            replacement_batch_writer,
        )

    def __build_collector(
        self,
    ) -> ProcessingStep[
        Sequence[Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]]
    ]:
        dead_letter_step: Optional[DeadLetterStep] = None
        if self.__producer and self.__topic:
            dead_letter_step = DeadLetterStep(
                producer=self.__producer, topic=self.__topic, metrics=self.__metrics,
            )
        return MultistorageCollector(
            {
                storage.get_storage_key(): self.__build_batch_writer(storage)
                for storage in self.__storages
            },
            ignore_errors={
                storage.get_storage_key()
                for storage in self.__storages
                if storage.get_is_write_error_ignorable() is True
            },
            dead_letter_step=dead_letter_step,
        )

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[KafkaPayload]:
        # 1. Identify the storages that should be used for the input message.
        # 2. Filter out any messages that do not apply to any storage.
        # 3. Transform the messages using the selected storages.
        # 4. Route the messages to the collector for each storage.
        collect = CollectStep(
            self.__build_collector,
            commit,
            self.__max_batch_size,
            self.__max_batch_time,
        )

        strategy: ProcessingStrategy[MultistorageKafkaPayload]

        if self.__processes is None:
            strategy = TransformStep(self.__process_message_fn, collect)
        else:
            assert self.__input_block_size is not None
            assert self.__output_block_size is not None
            strategy = ParallelTransformStep(
                self.__process_message_fn,
                collect,
                self.__processes,
                self.__max_batch_size,
                self.__max_batch_time,
                self.__input_block_size,
                self.__output_block_size,
                initializer=setup_sentry,
            )

        return TransformStep(
            self.__find_destination_storages,
            FilterStep(self.__has_destination_storages, strategy),
        )
