from datetime import datetime
from unittest import mock

import pytest

from snuba.utils.streams.processing import InvalidStateError, StreamProcessor
from snuba.utils.streams.types import Message, Partition, Topic
from tests.assertions import assert_changes


def test_stream_processor_lifecycle() -> None:
    topic = Topic("topic")

    consumer = mock.Mock()
    strategy = mock.Mock()
    factory = mock.Mock()
    factory.create.return_value = strategy

    with assert_changes(lambda: consumer.subscribe.call_count, 0, 1):
        processor: StreamProcessor[int] = StreamProcessor(consumer, topic, factory)

    # The processor should accept heartbeat messages without an assignment or
    # active processor.
    consumer.poll.return_value = None
    processor._run_once()

    # The processor should not accept non-heartbeat messages without an
    # assignment or active processor.
    message = Message(Partition(topic, 0), 0, 0, datetime.now())
    consumer.poll.return_value = message
    with pytest.raises(InvalidStateError):
        processor._run_once()

    # XXX: ``call().args``, ``call().kwargs`` are not available until 3.8
    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]

    assignment_callback = subscribe_kwargs["on_assign"]
    revocation_callback = subscribe_kwargs["on_revoke"]

    # Assignment should succeed if no assignment already exxists.
    assignment_callback({Partition(topic, 0): 0})

    with assert_changes(lambda: strategy.process.call_count, 0, 1):
        consumer.poll.return_value = None
        processor._run_once()
        assert strategy.process.call_args_list[-1] == mock.call(None)

    with assert_changes(lambda: strategy.process.call_count, 1, 2):
        consumer.poll.return_value = message
        processor._run_once()
        assert strategy.process.call_args_list[-1] == mock.call(message)

    # Assignment should fail if one already exists.
    with pytest.raises(InvalidStateError):
        assignment_callback({Partition(topic, 0): 0})

    # Revocation should succeed with an active assignment, and cause the
    # strategy instance to be closed.
    with assert_changes(lambda: strategy.close.call_count, 0, 1):
        revocation_callback([Partition(topic, 0)])

    # Revocation should fail without an active assignment.
    with pytest.raises(InvalidStateError):
        revocation_callback([Partition(topic, 0)])

    with assert_changes(lambda: consumer.close.call_count, 0, 1):
        processor._shutdown()
