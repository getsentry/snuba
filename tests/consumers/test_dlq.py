import pytest

from snuba.consumers.dlq import (
    DlqInstruction,
    DlqPolicy,
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
