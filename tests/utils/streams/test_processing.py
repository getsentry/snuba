from datetime import datetime
from unittest import mock

import pytest

from snuba.utils.streams.processing import (
    InvalidStateError,
    MessageRejected,
    StreamProcessor,
)
from snuba.utils.streams.types import Message, Partition, Topic
from tests.assertions import assert_changes, assert_does_not_change


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

    message = Message(Partition(topic, 0), 0, 0, datetime.now())

    # XXX: ``call().args``, ``call().kwargs`` are not available until 3.8
    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]

    assignment_callback = subscribe_kwargs["on_assign"]
    revocation_callback = subscribe_kwargs["on_revoke"]

    # Assignment should succeed if no assignment already exxists.
    offsets = {Partition(topic, 0): 0}
    assignment_callback(offsets)

    # If ``Consumer.poll`` doesn't return a message, we should poll the
    # processing strategy, but not submit anything for processing.
    consumer.poll.return_value = None
    with assert_changes(lambda: strategy.poll.call_count, 0, 1), assert_does_not_change(
        lambda: strategy.submit.call_count, 0
    ):
        processor._run_once()

    # If ``Consumer.poll`` **does** return a message, we should poll the
    # processing strategy and submit the message for processing.
    consumer.poll.return_value = message
    with assert_changes(lambda: strategy.poll.call_count, 1, 2), assert_changes(
        lambda: strategy.submit.call_count, 0, 1
    ):
        processor._run_once()
        assert strategy.submit.call_args_list[-1] == mock.call(message)

    # If the message is rejected by the processing strategy, the consumer
    # should be paused and the message should be held for later.
    consumer.tell.return_value = offsets
    consumer.poll.return_value = message
    strategy.submit.side_effect = MessageRejected()
    with assert_changes(lambda: consumer.pause.call_count, 0, 1):
        processor._run_once()
        assert strategy.submit.call_args_list[-1] == mock.call(message)

    # If ``Consumer.poll`` returns a message when we expect it to be paused,
    # we should raise an exception.
    with pytest.raises(InvalidStateError):
        processor._run_once()

    # Once the message is accepted by the processing strategy, the consumer
    # should be resumed.
    consumer.poll.return_value = None
    strategy.submit.return_value = None
    strategy.submit.side_effect = None
    with assert_changes(lambda: consumer.resume.call_count, 0, 1):
        processor._run_once()
        assert strategy.submit.call_args_list[-1] == mock.call(message)

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

    # The processor should not accept non-heartbeat messages without an
    # assignment or active processor.
    consumer.poll.return_value = message
    with pytest.raises(InvalidStateError):
        processor._run_once()

    with assert_changes(lambda: consumer.close.call_count, 0, 1):
        processor._shutdown()


def test_stream_processor_termination_on_error() -> None:
    topic = Topic("test")

    consumer = mock.Mock()
    consumer.poll.return_value = Message(Partition(topic, 0), 0, 0, datetime.now())

    exception = NotImplementedError("error")

    strategy = mock.Mock()
    strategy.submit.side_effect = exception

    factory = mock.Mock()
    factory.create.return_value = strategy

    processor: StreamProcessor[int] = StreamProcessor(consumer, topic, factory)

    assignment_callback = consumer.subscribe.call_args.kwargs["on_assign"]
    assignment_callback({Partition(topic, 0): 0})

    with pytest.raises(Exception) as e, assert_changes(
        lambda: strategy.terminate.call_count, 0, 1
    ), assert_changes(lambda: consumer.close.call_count, 0, 1):
        processor.run()

    assert e.value == exception
