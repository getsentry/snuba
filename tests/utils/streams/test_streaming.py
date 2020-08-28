import itertools
import pytest
from datetime import datetime
from multiprocessing.managers import SharedMemoryManager
from typing import Iterator
from unittest.mock import Mock, call

from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.streaming import (
    CollectStep,
    FilterStep,
    MessageBatch,
    TransformStep,
    ValueTooLarge,
)
from snuba.utils.streams.types import Message, Partition, Topic
from tests.assertions import assert_changes, assert_does_not_change


def test_filter() -> None:
    next_step = Mock()

    def test_function(message: Message[bool]) -> bool:
        return message.payload

    filter_step = FilterStep(test_function, next_step)

    fail_message = Message(Partition(Topic("topic"), 0), 0, False, datetime.now())

    with assert_does_not_change(lambda: next_step.submit.call_count, 0):
        filter_step.submit(fail_message)

    pass_message = Message(Partition(Topic("topic"), 0), 0, True, datetime.now())

    with assert_changes(lambda: next_step.submit.call_count, 0, 1):
        filter_step.submit(pass_message)

    assert next_step.submit.call_args == call(pass_message)

    with assert_changes(lambda: next_step.poll.call_count, 0, 1):
        filter_step.poll()

    with assert_changes(lambda: next_step.close.call_count, 0, 1), assert_changes(
        lambda: next_step.join.call_count, 0, 1
    ):
        filter_step.join()


def test_transform() -> None:
    next_step = Mock()

    def transform_function(message: Message[int]) -> int:
        return message.payload * 2

    transform_step = TransformStep(transform_function, next_step)

    original_message = Message(Partition(Topic("topic"), 0), 0, 1, datetime.now())

    with assert_changes(lambda: next_step.submit.call_count, 0, 1):
        transform_step.submit(original_message)

    assert next_step.submit.call_args == call(
        Message(
            original_message.partition,
            original_message.offset,
            transform_function(original_message),
            original_message.timestamp,
        )
    )

    with assert_changes(lambda: next_step.poll.call_count, 0, 1):
        transform_step.poll()

    with assert_changes(lambda: next_step.close.call_count, 0, 1), assert_changes(
        lambda: next_step.join.call_count, 0, 1
    ):
        transform_step.join()


def message_generator(
    partition: Partition, starting_offset: int = 0
) -> Iterator[Message[int]]:
    for i in itertools.count(starting_offset):
        yield Message(partition, i, i, datetime.now())


def test_collect() -> None:
    step_factory = Mock()
    step_factory.return_value = inner_step = Mock()

    commit_function = Mock()
    partition = Partition(Topic("topic"), 0)
    messages = message_generator(partition, 0)

    collect_step = CollectStep(step_factory, commit_function, 2, 60)

    # A batch should be started the first time the step receives a message.
    with assert_changes(lambda: step_factory.call_count, 0, 1):
        collect_step.poll()
        collect_step.submit(next(messages))  # offset 0

    # Subsequent messages should reuse the existing batch, ...
    with assert_does_not_change(lambda: step_factory.call_count, 1):
        collect_step.poll()
        collect_step.submit(next(messages))  # offset 1

    # ...until we hit the batch size limit.
    with assert_changes(lambda: inner_step.close.call_count, 0, 1), assert_changes(
        lambda: inner_step.join.call_count, 0, 1
    ), assert_changes(lambda: commit_function.call_count, 0, 1):
        collect_step.poll()
        assert commit_function.call_args == call({partition: 2})

    step_factory.return_value = inner_step = Mock()

    # The next message should create a new batch.
    with assert_changes(lambda: step_factory.call_count, 1, 2):
        collect_step.submit(next(messages))

    with assert_changes(lambda: inner_step.close.call_count, 0, 1):
        collect_step.close()

    with assert_changes(lambda: inner_step.join.call_count, 0, 1), assert_changes(
        lambda: commit_function.call_count, 1, 2
    ):
        collect_step.join()


def test_message_batch() -> None:
    partition = Partition(Topic("test"), 0)
    with SharedMemoryManager() as smm:
        block = smm.SharedMemory(4096)
        assert block.size == 4096

        message = Message(
            partition, 0, KafkaPayload(None, b"\x00" * 4000, None), datetime.now()
        )

        batch: MessageBatch[KafkaPayload] = MessageBatch(block)
        with assert_changes(lambda: len(batch), 0, 1):
            batch.append(message)

        assert batch[0] == message
        assert list(batch) == [message]

        with assert_does_not_change(lambda: len(batch), 1), pytest.raises(
            ValueTooLarge
        ):
            batch.append(message)
