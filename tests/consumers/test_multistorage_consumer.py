import json
from datetime import datetime

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload

from snuba.consumers.consumer import (
    MultistorageKafkaPayload,
    process_message_multistorage,
)
from snuba.datasets.storages import StorageKey
from tests.fixtures import get_raw_event, get_raw_transaction

test_data = [
    pytest.param(
        Message(
            Partition(Topic("errors"), 1),
            1,
            MultistorageKafkaPayload(
                [StorageKey.ERRORS, StorageKey.ERRORS_V2],
                KafkaPayload(
                    None,
                    json.dumps((2, "insert", get_raw_event())).encode("utf-8"),
                    [],
                ),
            ),
            datetime.now(),
        ),
        True,
        id="both errors storage",
    ),
    pytest.param(
        Message(
            Partition(Topic("errors"), 1),
            1,
            MultistorageKafkaPayload(
                [StorageKey.TRANSACTIONS, StorageKey.TRANSACTIONS_V2],
                KafkaPayload(
                    None,
                    json.dumps((2, "insert", get_raw_transaction())).encode("utf-8"),
                    [],
                ),
            ),
            datetime.now(),
        ),
        True,
        id="both transactions storage",
    ),
    pytest.param(
        Message(
            Partition(Topic("errors"), 1),
            1,
            MultistorageKafkaPayload(
                [StorageKey.ERRORS_V2, StorageKey.TRANSACTIONS_V2],
                KafkaPayload(
                    None,
                    json.dumps((2, "insert", get_raw_event())).encode("utf-8"),
                    [],
                ),
            ),
            datetime.now(),
        ),
        False,
        id="errors and transactions",
    ),
]


@pytest.mark.parametrize("message, expected", test_data)
def test_process_multistorage(
    message: Message[MultistorageKafkaPayload], expected: bool
) -> None:
    results = process_message_multistorage(message)
    assert len(results) == 2

    assert (results[0][1] == results[1][1]) is expected
