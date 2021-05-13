import itertools
import multiprocessing
from datetime import datetime
from multiprocessing.managers import SharedMemoryManager
from typing import Any, Iterator
from unittest.mock import Mock, call

import pytest

from snuba.utils.streams.backends.kafka import KafkaPayload
from snuba.utils.streams.processing.strategies.streaming.collect import CollectStep
from snuba.utils.streams.processing.strategies.streaming.filter import FilterStep
from snuba.utils.streams.processing.strategies.streaming.transform import (
    MessageBatch,
    ParallelTransformStep,
    TransformStep,
    ValueTooLarge,
    parallel_transform_worker_apply,
)
from snuba.utils.streams.types import Message, Partition, Topic
from tests.assertions import assert_changes, assert_does_not_change
from tests.backends.metrics import Gauge as GaugeCall
from tests.backends.metrics import TestingMetricsBackend
from tests.backends.metrics import Timing as TimingCall


def test_filter() -> None:
    next_step = Mock()

    def test_function(message: Message[bool]) -> bool:
        return message.payload

    filter_step = FilterStep(test_function, next_step)

    fail_message = Message(Partition(Topic("topic"), 0), 0, False, datetime.now())

    with assert_does_not_change(lambda: int(next_step.submit.call_count), 0):
        filter_step.submit(fail_message)

    pass_message = Message(Partition(Topic("topic"), 0), 0, True, datetime.now())

    with assert_changes(lambda: int(next_step.submit.call_count), 0, 1):
        filter_step.submit(pass_message)

    assert next_step.submit.call_args == call(pass_message)

    with assert_changes(lambda: int(next_step.poll.call_count), 0, 1):
        filter_step.poll()

    with assert_changes(lambda: int(next_step.close.call_count), 0, 1), assert_changes(
        lambda: int(next_step.join.call_count), 0, 1
    ):
        filter_step.join()


def test_transform() -> None:
    next_step = Mock()

    def transform_function(message: Message[int]) -> int:
        return message.payload * 2

    transform_step = TransformStep(transform_function, next_step)

    original_message = Message(Partition(Topic("topic"), 0), 0, 1, datetime.now())

    with assert_changes(lambda: int(next_step.submit.call_count), 0, 1):
        transform_step.submit(original_message)

    assert next_step.submit.call_args == call(
        Message(
            original_message.partition,
            original_message.offset,
            transform_function(original_message),
            original_message.timestamp,
        )
    )

    with assert_changes(lambda: int(next_step.poll.call_count), 0, 1):
        transform_step.poll()

    with assert_changes(lambda: int(next_step.close.call_count), 0, 1), assert_changes(
        lambda: int(next_step.join.call_count), 0, 1
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
    with assert_changes(lambda: int(inner_step.close.call_count), 0, 1), assert_changes(
        lambda: int(inner_step.join.call_count), 0, 1
    ), assert_changes(lambda: commit_function.call_count, 0, 1):
        collect_step.poll()
        assert commit_function.call_args == call({partition: 2})

    step_factory.return_value = inner_step = Mock()

    # The next message should create a new batch.
    with assert_changes(lambda: step_factory.call_count, 1, 2):
        collect_step.submit(next(messages))

    with assert_changes(lambda: int(inner_step.close.call_count), 0, 1):
        collect_step.close()

    with assert_changes(lambda: int(inner_step.join.call_count), 0, 1), assert_changes(
        lambda: commit_function.call_count, 1, 2
    ):
        collect_step.join()


def test_message_batch() -> None:
    partition = Partition(Topic("test"), 0)

    smm = SharedMemoryManager()
    smm.start()

    block = smm.SharedMemory(4096)
    assert block.size == 4096

    message = Message(
        partition, 0, KafkaPayload(None, b"\x00" * 4000, []), datetime.now()
    )

    batch: MessageBatch[KafkaPayload] = MessageBatch(block)
    with assert_changes(lambda: len(batch), 0, 1):
        batch.append(message)

    assert batch[0] == message
    assert list(batch) == [message]

    with assert_does_not_change(lambda: len(batch), 1), pytest.raises(ValueTooLarge):
        batch.append(message)

    smm.shutdown()


def transform_payload_expand(message: Message[KafkaPayload]) -> KafkaPayload:
    return KafkaPayload(
        message.payload.key, message.payload.value * 2, message.payload.headers,
    )


def test_parallel_transform_worker_apply() -> None:
    messages = [
        Message(
            Partition(Topic("test"), 0),
            i,
            KafkaPayload(None, b"\x00" * size, []),
            datetime.now(),
        )
        for i, size in enumerate([1000, 1000, 2000, 4000])
    ]

    smm = SharedMemoryManager()
    smm.start()
    input_block = smm.SharedMemory(8192)
    assert input_block.size == 8192

    input_batch = MessageBatch[Any](input_block)
    for message in messages:
        input_batch.append(message)

    assert len(input_batch) == 4

    output_block = smm.SharedMemory(4096)
    assert output_block.size == 4096

    index, output_batch = parallel_transform_worker_apply(
        transform_payload_expand, input_batch, output_block,
    )

    # The first batch should be able to fit 2 messages.
    assert index == 2
    assert len(output_batch) == 2

    index, output_batch = parallel_transform_worker_apply(
        transform_payload_expand, input_batch, output_block, index,
    )

    # The second batch should be able to fit one message.
    assert index == 3
    assert len(output_batch) == 1

    # The last message is too large to fit in the batch.
    with pytest.raises(ValueTooLarge):
        parallel_transform_worker_apply(
            transform_payload_expand, input_batch, output_block, index,
        )
    smm.shutdown()


def get_subprocess_count() -> int:
    return len(multiprocessing.active_children())


def test_parallel_transform_step() -> None:
    next_step = Mock()

    messages = [
        Message(
            Partition(Topic("test"), 0),
            i,
            KafkaPayload(None, b"\x00" * size, []),
            datetime.now(),
        )
        for i, size in enumerate([1000, 1000, 2000, 2000])
    ]

    starting_processes = get_subprocess_count()
    worker_processes = 2
    manager_processes = 1
    metrics = TestingMetricsBackend()

    with assert_changes(
        get_subprocess_count,
        starting_processes,
        starting_processes + worker_processes + manager_processes,
    ), assert_changes(
        lambda: metrics.calls,
        [],
        [
            GaugeCall("batches_in_progress", 0.0, tags=None),
            GaugeCall("batches_in_progress", 1.0, tags=None),
            TimingCall("batch.size.msg", 3, None),
            TimingCall("batch.size.bytes", 4000, None),
            GaugeCall("batches_in_progress", 2.0, tags=None),
            TimingCall("batch.size.msg", 1, None),
            TimingCall("batch.size.bytes", 2000, None),
        ],
    ):
        transform_step = ParallelTransformStep(
            transform_payload_expand,
            next_step,
            processes=worker_processes,
            max_batch_size=5,
            max_batch_time=60,
            input_block_size=4096,
            output_block_size=4096,
            metrics=metrics,
        )

        for message in messages:
            transform_step.poll()
            transform_step.submit(message)

        transform_step.close()

    metrics.calls.clear()

    with assert_changes(
        get_subprocess_count,
        starting_processes + worker_processes + manager_processes,
        starting_processes,
    ), assert_changes(
        lambda: metrics.calls,
        [],
        [GaugeCall("batches_in_progress", value, tags=None) for value in [1.0, 0.0]],
    ):
        transform_step.join()

    assert next_step.submit.call_count == len(messages)


def test_parallel_transform_step_terminate_workers() -> None:
    next_step = Mock()

    starting_processes = get_subprocess_count()
    worker_processes = 2
    manager_processes = 1

    with assert_changes(
        get_subprocess_count,
        starting_processes,
        starting_processes + worker_processes + manager_processes,
    ):
        transform_step = ParallelTransformStep(
            transform_payload_expand,  # doesn't matter
            next_step,
            processes=worker_processes,
            max_batch_size=5,
            max_batch_time=60,
            input_block_size=4096,
            output_block_size=4096,
            metrics=TestingMetricsBackend(),
        )

    with assert_changes(
        get_subprocess_count,
        starting_processes + worker_processes + manager_processes,
        starting_processes,
    ), assert_changes(lambda: int(next_step.terminate.call_count), 0, 1):
        transform_step.terminate()
