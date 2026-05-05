from __future__ import annotations

from datetime import datetime
from typing import Any
from unittest.mock import Mock, patch

import rapidjson
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba.lw_deletions.batching import BatchStepCustom, ReduceRowsBuffer
from snuba.lw_deletions.strategy import increment_by
from snuba.utils.streams.topics import Topic as SnubaTopic


def _lw_deletion_kafka_message(offset: int) -> Message[KafkaPayload]:
    payload = {
        "rows_to_delete": 5,
        "storage_name": "search_issues",
        "conditions": {"project_id": [1]},
        "tenant_ids": {"project_id": 1, "organization_id": 1},
    }
    return Message(
        BrokerValue(
            KafkaPayload(None, rapidjson.dumps(payload).encode("utf-8"), []),
            Partition(Topic(SnubaTopic.LW_DELETIONS_GENERIC_EVENTS.value), 0),
            offset,
            datetime(1970, 1, 1),
        )
    )


@patch("snuba.lw_deletions.batching.MAX_BUFFERED_MESSAGES", 3)
def test_reduce_rows_buffer_is_ready_after_buffered_messages_exceeds_max() -> None:
    def accumulator(acc: list[object], msg: object) -> list[object]:
        acc.append(msg)
        return acc

    buf: ReduceRowsBuffer[Any, list[Any]] = ReduceRowsBuffer(
        accumulator,
        lambda: [],
        max_batch_size=10**9,
        max_batch_time=10**9,
    )
    assert buf.is_empty
    buf.append(Mock())
    assert not buf.is_ready
    buf.append(Mock())
    assert not buf.is_ready
    buf.append(Mock())
    assert buf.is_ready

    # sudo flush the batch, test that state is reset
    buf = buf.new()
    assert not buf.is_ready
    buf.append(Mock())
    assert not buf.is_ready


@patch("snuba.lw_deletions.batching.MAX_BUFFERED_MESSAGES", 5)
def test_rows_to_delete_and_max_buffered_messages() -> None:
    """
    Buffer.submit flushes when is_ready before accepting the next message.
    with rows_to_delete as 5 for each message it will batch 3 messages and
    the next batch will be the last message.
    Test that max_batch_size unaffected by MAX_BUFFERED_MESSAGES.
    """
    next_step = Mock()
    strategy: BatchStepCustom[KafkaPayload] = BatchStepCustom(
        max_batch_size=15,
        max_batch_time=10**9,
        next_step=next_step,
        increment_by=increment_by,
    )
    for i in range(3):
        strategy.submit(_lw_deletion_kafka_message(i))
    assert next_step.submit.call_count == 0

    # new batch created after submitting
    strategy.submit(_lw_deletion_kafka_message(4))
    assert next_step.submit.call_count == 1
    flushed = next_step.submit.call_args[0][0]
    assert len(flushed.payload) == 3

    strategy.close()
    strategy.join()
    assert next_step.submit.call_count == 2
    final = next_step.submit.call_args[0][0]
    assert len(final.payload) == 1
