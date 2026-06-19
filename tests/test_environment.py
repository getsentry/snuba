from __future__ import annotations

import logging
from typing import cast

from sentry_sdk.types import Event, Hint

from snuba.environment import before_send


def _log_record(name: str, message: str) -> logging.LogRecord:
    return logging.LogRecord(
        name=name,
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )


def _hint(record: logging.LogRecord) -> Hint:
    return cast(Hint, {"log_record": record})


def test_before_send_drops_transient_rebalance_commit_failures() -> None:
    for code in ("UNKNOWN_MEMBER_ID", "REBALANCE_IN_PROGRESS", "ILLEGAL_GENERATION"):
        record = _log_record(
            "arroyo.backends.kafka.consumer",
            f'Commit failed: KafkaError{{code={code},val=25,str="Broker: ..."}}. '
            "Partitions: ['group-attributes:8']",
        )
        assert before_send(cast(Event, {}), _hint(record)) is None


def test_before_send_keeps_genuine_kafka_commit_failures() -> None:
    record = _log_record(
        "arroyo.backends.kafka.consumer",
        "Commit failed: KafkaError{code=BROKER_NOT_AVAILABLE}",
    )
    event = cast(Event, {"message": "Commit failed"})
    assert before_send(event, _hint(record)) is event


def test_before_send_keeps_other_loggers() -> None:
    record = _log_record("snuba.something", "Commit failed: UNKNOWN_MEMBER_ID")
    event = cast(Event, {"message": "boom"})
    assert before_send(event, _hint(record)) is event
