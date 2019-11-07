from unittest.mock import Mock, sentinel

from snuba.utils.retries import NoRetryPolicy, BasicRetryPolicy, constant_delay
from snuba.utils.streams.consumers.backends.abstract import ConsumerBackend
from snuba.utils.streams.consumers.consumer import Consumer


def test_commit_retry_no_policy() -> None:
    backend = Mock(spec=ConsumerBackend)
    consumer = Consumer(backend, commit_retry_policy=NoRetryPolicy())

    backend.commit.side_effect = Exception("error")

    try:
        consumer.commit()
    except Exception as e:
        assert e is backend.commit.side_effect
    else:
        assert False, "expected commit to raise"

    assert backend.commit.call_count == 1


def test_commit_retry_basic_policy() -> None:
    backend = Mock(spec=ConsumerBackend)
    consumer = Consumer(
        backend, commit_retry_policy=BasicRetryPolicy(3, constant_delay(1.0))
    )

    backend.commit.side_effect = [Exception("error"), sentinel.COMMIT_RESULT]

    assert consumer.commit() is sentinel.COMMIT_RESULT
    assert backend.commit.call_count == 2
