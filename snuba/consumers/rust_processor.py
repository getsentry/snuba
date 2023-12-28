# This file is loaded from within Rust in a subprocess with certain environment
# variables:
#
# python interpreter
#   runs: rust snuba CLI
#     calls: rust_snuba crate's consumer main function, exposed via PyO3
#       spawns: N subprocesses via fork()
#         sets environment variables and imports: this module
#
# importing this file at the outer subprocess will crash due to missing
# environment variables


import importlib
import logging
import os
from collections import deque
from datetime import datetime
from typing import Deque, Mapping, Optional, Sequence, Tuple, Type, Union, cast

import rapidjson
from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.processing.strategies.run_task_with_multiprocessing import (
    MultiprocessingPool,
    RunTaskWithMultiprocessing,
)
from arroyo.types import BrokerValue, FilteredPayload, Message, Partition, Topic

from snuba.consumers.consumer import json_row_encoder
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch

logger = logging.getLogger(__name__)

processor: Optional[DatasetMessageProcessor] = None


def initialize_processor(
    module: Optional[str] = None, classname: Optional[str] = None
) -> None:
    if not module or not classname:
        module = os.environ.get("RUST_SNUBA_PROCESSOR_MODULE")
        classname = os.environ.get("RUST_SNUBA_PROCESSOR_CLASSNAME")

    if not module or not classname:
        return

    module_object = importlib.import_module(module)
    Processor: Type[DatasetMessageProcessor] = getattr(module_object, classname)

    global processor
    processor = Processor()


initialize_processor()


def process_rust_message(
    message: bytes, offset: int, partition: int, timestamp: datetime
) -> Tuple[Sequence[bytes], Optional[datetime], Optional[datetime]]:
    if processor is None:
        raise RuntimeError("processor not yet initialized")
    rv = processor.process_message(
        rapidjson.loads(bytearray(message)),
        KafkaMessageMetadata(offset=offset, partition=partition, timestamp=timestamp),
    )

    if rv is None:
        return [], None, None

    assert isinstance(rv, InsertBatch), "this consumer does not support replacements"

    return (
        [json_row_encoder.encode(row) for row in rv.rows],
        rv.origin_timestamp,
        rv.sentry_received_timestamp,
    )


Committable = Mapping[Tuple[str, int], int]

MessageTimestamp = datetime

ReturnValue = Tuple[Sequence[bytes], Optional[datetime], Optional[datetime]]

ReturnValueWithCommittable = Tuple[ReturnValue, MessageTimestamp, Committable]


def wrap_process_message(
    message: Message[bytes],
) -> Union[FilteredPayload, ReturnValue]:
    value = message.value
    assert isinstance(value, BrokerValue)

    return process_rust_message(
        value.payload, value.offset, value.partition.index, value.timestamp
    )


class TransformedMessages:
    """
    Transformed messages to be returned to Rust
    """

    def __init__(self) -> None:
        self.messages: Deque[Message[ReturnValue]] = deque()

    def append(self, value: Message[ReturnValue]) -> None:
        self.messages.append(value)

    def pop(self) -> Sequence[Message[ReturnValue]]:
        """
        Returns all the transformed messages and clears the internal buffer
        """
        messages = self.messages
        self.messages = deque()
        return messages


class Next(ProcessingStrategy[Union[FilteredPayload, ReturnValue]]):
    """
    Messages are passed to this step from RunTaskWithMultiprocessing,
    and they are added to transformed messages. They are removed and
    passed back to Rust by the RunPythonMultiprocessing class.
    """

    def __init__(self, transformed_messages: TransformedMessages) -> None:
        self.__transformed_messages = transformed_messages

    def submit(self, message: Message[Union[FilteredPayload, ReturnValue]]) -> None:
        self.__transformed_messages.append(cast(Message[ReturnValue], message))

    def poll(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        pass

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass


DEFAULT_BLOCK_SIZE = int(32 * 1e6)


class RunPythonMultiprocessing:
    """
    Similar to Arroyo's RunTaskWithMultiprocessing class (which it uses internally),
    but instead of submitting messages to a nested processing strategy, returns
    transformed rows in poll() and join() instead. This makes it easier to use from
    Rust.
    """

    def __init__(self, concurrency: int, max_queue_depth: int) -> None:
        self.__transformed_messages = TransformedMessages()
        self.__next = Next(self.__transformed_messages)
        transform_fn = wrap_process_message
        self.__pool = MultiprocessingPool(concurrency)
        # Message is carried over if we got MessageRejected from the next step
        self.__carried_over_message: Optional[Message[bytes]] = None

        self.__inner = RunTaskWithMultiprocessing(
            transform_fn,
            self.__next,
            max_queue_depth,
            1.0,
            self.__pool,
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_SIZE,
        )

    def submit(
        self,
        payload: bytes,
        topic: str,
        offset: int,
        partition: int,
        timestamp: datetime,
    ) -> int:
        # HACK: There is probably a better way to handle exceptions in Rust
        # 0 means message successfully submitted
        # 1 means backpressure
        # Invalid message is not currently supported
        if self.__carried_over_message is not None:
            return 1

        message = Message(
            BrokerValue(payload, Partition(Topic(topic), partition), offset, timestamp)
        )
        try:
            self.__inner.submit(message)
        except InvalidMessage:
            logger.warning("Dropping invalid message", exc_info=True)
        except MessageRejected:
            self.__carried_over_message = message

        return 0

    def __get_transformed_messages(self) -> Sequence[ReturnValueWithCommittable]:
        return [
            (
                m.payload,
                cast(datetime, m.timestamp),
                {
                    (partition.topic.name, partition.index): offset
                    for (partition, offset) in m.committable.items()
                },
            )
            for m in self.__transformed_messages.pop()
        ]

    def poll(self) -> Sequence[ReturnValueWithCommittable]:
        """
        Returns all available transformed rows
        """
        try:
            self.__inner.poll()
        except InvalidMessage:
            logger.warning("Dropping invalid message", exc_info=True)

        return self.__get_transformed_messages()

    def join(
        self, timeout: Optional[float] = None
    ) -> Sequence[ReturnValueWithCommittable]:
        """
        Close and join inner strategy. Returns all available transformed rows
        """
        self.__inner.close()
        try:
            self.__inner.join(timeout=timeout)
        except InvalidMessage:
            # We cannot forward offsets here since the inner strategy is already
            # marked as closed. Log the exception and move on. The message will get
            # reprocessed properly when the consumer is restarted.
            logger.warning("Invalid message in join", exc_info=True)
        self.__pool.close()

        return self.__get_transformed_messages()

    def terminate(self) -> None:
        self.__inner.terminate()
