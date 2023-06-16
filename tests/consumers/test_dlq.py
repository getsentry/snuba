from datetime import datetime
from unittest.mock import Mock

import pytest
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba.consumers.dlq import (
    DlqInstruction,
    DlqPolicy,
    ExitAfterNMessages,
    clear_instruction,
    load_instruction,
    store_instruction,
)
from snuba.datasets.storages.storage_key import StorageKey


def test_dlq_instruction() -> None:
    instruction = DlqInstruction(DlqPolicy.STOP_ON_ERROR, StorageKey.QUERYLOG, None, 1)
    encoded = instruction.to_bytes()
    assert DlqInstruction.from_bytes(encoded) == instruction


@pytest.mark.redis_db
def test_store_instruction() -> None:
    instruction = DlqInstruction(DlqPolicy.STOP_ON_ERROR, StorageKey.QUERYLOG, None, 1)
    store_instruction(instruction)
    assert load_instruction() == instruction
    clear_instruction()
    assert load_instruction() is None


def test_exit_after_n_messages() -> None:
    next_step = Mock()
    num_messages_to_process = 10
    max_message_timeout = 1.0
    strategy: ProcessingStrategy[int] = ExitAfterNMessages(
        next_step, num_messages_to_process, max_message_timeout
    )

    topic = Topic("topic")
    partition = Partition(topic, 0)

    for i in range(num_messages_to_process - 1):
        strategy.submit(Message(BrokerValue(i, partition, i, datetime.now())))
        strategy.poll()

    strategy.submit(Message(BrokerValue(i, partition, i, datetime.now())))
    with pytest.raises(KeyboardInterrupt):
        strategy.poll()
