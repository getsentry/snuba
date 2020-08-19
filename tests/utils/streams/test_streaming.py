from datetime import datetime
from unittest.mock import Mock, call

from snuba.utils.streams.types import Message, Partition, Topic
from snuba.utils.streams.streaming import FilterStep, TransformStep
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
