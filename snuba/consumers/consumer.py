import functools
import itertools
import logging
import time
from datetime import datetime
from pickle import PickleBuffer
from typing import (
    Any,
    Callable,
    Mapping,
    MutableSequence,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import rapidjson
from confluent_kafka import Producer as ConfluentKafkaProducer
from snuba.clickhouse.http import JSONRow, JSONRowEncoder
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.message_filters import StreamMessageFilter
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.processor import InsertBatch, MessageProcessor, ReplacementBatch
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams import Message, Partition, Topic
from snuba.utils.streams.backends.kafka import (
    KafkaPayload,
    build_kafka_producer_configuration,
)
from snuba.utils.streams.processing.strategies import ProcessingStrategy
from snuba.utils.streams.processing.strategies import (
    ProcessingStrategy as ProcessingStep,
)
from snuba.utils.streams.processing.strategies import ProcessingStrategyFactory
from snuba.utils.streams.processing.strategies.streaming import (
    CollectStep,
    FilterStep,
    ParallelTransformStep,
    TransformStep,
)
from snuba.writer import BatchWriter

logger = logging.getLogger("snuba.consumer")


class JSONRowInsertBatch(NamedTuple):
    rows: Sequence[JSONRow]
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


class InsertBatchWriter(ProcessingStep[JSONRowInsertBatch]):
    def __init__(self, writer: BatchWriter[JSONRow], metrics: MetricsBackend) -> None:
        self.__writer = writer
        self.__metrics = metrics

        self.__messages: MutableSequence[Message[JSONRowInsertBatch]] = []
        self.__closed = False

    def poll(self) -> None:
        pass

    def submit(self, message: Message[JSONRowInsertBatch]) -> None:
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

        logger.debug(
            "Waited %0.4f seconds for %r rows to be written to %r.",
            write_finish - write_start,
            sum(len(message.payload.rows) for message in self.__messages),
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
    ProcessingStep[Union[None, JSONRowInsertBatch, ReplacementBatch]]
):
    def __init__(
        self,
        insert_batch_writer: InsertBatchWriter,
        replacement_batch_writer: Optional[ReplacementBatchWriter] = None,
    ) -> None:
        self.__insert_batch_writer = insert_batch_writer
        self.__replacement_batch_writer = replacement_batch_writer

        self.__closed = False

    def poll(self) -> None:
        self.__insert_batch_writer.poll()

        if self.__replacement_batch_writer is not None:
            self.__replacement_batch_writer.poll()

    def submit(
        self, message: Message[Union[None, JSONRowInsertBatch, ReplacementBatch]]
    ) -> None:
        assert not self.__closed

        if message.payload is None:
            return

        if isinstance(message.payload, JSONRowInsertBatch):
            self.__insert_batch_writer.submit(
                cast(Message[JSONRowInsertBatch], message)
            )
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


def process_message(
    processor: MessageProcessor, message: Message[KafkaPayload]
) -> Union[None, JSONRowInsertBatch, ReplacementBatch]:
    result = processor.process_message(
        rapidjson.loads(message.payload.value),
        KafkaMessageMetadata(
            message.offset, message.partition.index, message.timestamp
        ),
    )

    if isinstance(result, InsertBatch):
        return JSONRowInsertBatch(
            [json_row_encoder.encode(row) for row in result.rows],
            result.origin_timestamp,
        )
    else:
        return result


class StreamingConsumerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
    def __init__(
        self,
        prefilter: Optional[StreamMessageFilter[KafkaPayload]],
        processor: MessageProcessor,
        writer: BatchWriter[JSONRow],
        metrics: MetricsBackend,
        max_batch_size: int,
        max_batch_time: float,
        processes: Optional[int],
        input_block_size: Optional[int],
        output_block_size: Optional[int],
        replacements_producer: Optional[ConfluentKafkaProducer] = None,
        replacements_topic: Optional[Topic] = None,
    ) -> None:
        self.__prefilter = prefilter
        self.__processor = processor
        self.__writer = writer
        self.__metrics = metrics

        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time

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

        self.__processes = processes
        self.__input_block_size = input_block_size
        self.__output_block_size = output_block_size

        assert not (replacements_producer is None) ^ (replacements_topic is None)
        self.__supports_replacements = replacements_producer is not None
        self.__replacements_producer = replacements_producer
        self.__replacements_topic = replacements_topic

    def __should_accept(self, message: Message[KafkaPayload]) -> bool:
        assert self.__prefilter is not None
        return not self.__prefilter.should_drop(message)

    def __build_write_step(self) -> ProcessedMessageBatchWriter:
        insert_batch_writer = InsertBatchWriter(
            self.__writer, MetricsWrapper(self.__metrics, "insertions")
        )

        replacement_batch_writer: Optional[ReplacementBatchWriter]
        if self.__supports_replacements:
            assert self.__replacements_producer is not None
            assert self.__replacements_topic is not None
            replacement_batch_writer = ReplacementBatchWriter(
                self.__replacements_producer, self.__replacements_topic
            )
        else:
            replacement_batch_writer = None

        return ProcessedMessageBatchWriter(
            insert_batch_writer, replacement_batch_writer
        )

    def create(
        self, commit: Callable[[Mapping[Partition, int]], None]
    ) -> ProcessingStrategy[KafkaPayload]:
        collect = CollectStep(
            self.__build_write_step,
            commit,
            self.__max_batch_size,
            self.__max_batch_time,
        )

        transform_function = functools.partial(process_message, self.__processor)

        strategy: ProcessingStrategy[KafkaPayload]
        if self.__processes is None:
            strategy = TransformStep(transform_function, collect)
        else:
            assert self.__input_block_size is not None
            assert self.__output_block_size is not None
            strategy = ParallelTransformStep(
                transform_function,
                collect,
                self.__processes,
                max_batch_size=self.__max_batch_size,
                max_batch_time=self.__max_batch_time,
                input_block_size=self.__input_block_size,
                output_block_size=self.__output_block_size,
                metrics=MetricsWrapper(self.__metrics, "process"),
            )

        if self.__prefilter is not None:
            strategy = FilterStep(self.__should_accept, strategy)

        return strategy


class MultistorageCollector(
    ProcessingStep[
        Sequence[Tuple[StorageKey, Union[None, JSONRowInsertBatch, ReplacementBatch]]]
    ]
):
    def __init__(
        self,
        steps: Mapping[
            StorageKey,
            ProcessingStep[Union[None, JSONRowInsertBatch, ReplacementBatch]],
        ],
    ):
        self.__steps = steps

        self.__closed = False

    def poll(self) -> None:
        for step in self.__steps.values():
            step.poll()

    def submit(
        self,
        message: Message[
            Sequence[
                Tuple[StorageKey, Union[None, JSONRowInsertBatch, ReplacementBatch]]
            ]
        ],
    ) -> None:
        assert not self.__closed

        for storage_key, payload in message.payload:
            self.__steps[storage_key].submit(
                Message(
                    message.partition,
                    message.offset,
                    payload,
                    message.timestamp,
                    message.next_offset,
                )
            )

    def close(self) -> None:
        self.__closed = True

        for step in self.__steps.values():
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


class MultistorageKafkaPayload(NamedTuple):
    storage_keys: Sequence[StorageKey]
    payload: KafkaPayload


def process_message_multistorage(
    message: Message[MultistorageKafkaPayload],
) -> Sequence[Tuple[StorageKey, Union[None, JSONRowInsertBatch, ReplacementBatch]]]:
    # XXX: Avoid circular import on KafkaMessageMetadata, remove when that type
    # is itself removed.
    from snuba.datasets.storages.factory import get_writable_storage

    value = rapidjson.loads(message.payload.payload.value)
    metadata = KafkaMessageMetadata(
        message.offset, message.partition.index, message.timestamp
    )

    results: MutableSequence[
        Tuple[StorageKey, Union[None, JSONRowInsertBatch, ReplacementBatch]]
    ] = []

    for storage_key in message.payload.storage_keys:
        result = (
            get_writable_storage(storage_key)
            .get_table_writer()
            .get_stream_loader()
            .get_processor()
            .process_message(value, metadata)
        )
        if isinstance(result, InsertBatch):
            results.append(
                (
                    storage_key,
                    JSONRowInsertBatch(
                        [json_row_encoder.encode(row) for row in result.rows],
                        result.origin_timestamp,
                    ),
                )
            )
        else:
            results.append((storage_key, result))

    return results


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
                        storage.get_storage_key(),
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
        Sequence[Tuple[StorageKey, Union[None, JSONRowInsertBatch, ReplacementBatch]]]
    ]:
        return MultistorageCollector(
            {
                storage.get_storage_key(): self.__build_batch_writer(storage)
                for storage in self.__storages
            }
        )

    def create(
        self, commit: Callable[[Mapping[Partition, int]], None]
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
            strategy = TransformStep(process_message_multistorage, collect)
        else:
            assert self.__input_block_size is not None
            assert self.__output_block_size is not None
            strategy = ParallelTransformStep(
                process_message_multistorage,
                collect,
                self.__processes,
                self.__max_batch_size,
                self.__max_batch_time,
                self.__input_block_size,
                self.__output_block_size,
                self.__metrics,
            )

        return TransformStep(
            self.__find_destination_storages,
            FilterStep(self.__has_destination_storages, strategy,),
        )
