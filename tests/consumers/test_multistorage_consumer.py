import json
from datetime import datetime

import pytest
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba.consumers.consumer import (
    MultistorageKafkaPayload,
    process_message_multistorage,
    process_message_multistorage_identical_storages,
)
from snuba.datasets.storages.storage_key import StorageKey
from tests.fixtures import get_raw_event

test_data = [
    pytest.param(
        Message(
            BrokerValue(
                MultistorageKafkaPayload(
                    [StorageKey.ERRORS, StorageKey.TRANSACTIONS],
                    KafkaPayload(
                        None,
                        json.dumps((2, "insert", get_raw_event())).encode("utf-8"),
                        [],
                    ),
                ),
                Partition(Topic("errors"), 1),
                1,
                datetime.now(),
            )
        ),
        False,
        id="errors and transactions",
    ),
]


@pytest.mark.parametrize("message, expected", test_data)
def test_process_multistorage_identical_write_returns_same_result_object(
    message: Message[MultistorageKafkaPayload], expected: bool
) -> None:
    results = process_message_multistorage_identical_storages(message)
    assert len(results) == 2

    assert (id(results[0][1]) == id(results[1][1])) is expected


@pytest.mark.parametrize("message, ignored", test_data)
def test_process_multistorage_variants_return_same_result(
    message: Message[MultistorageKafkaPayload], ignored: bool
) -> None:
    result1 = process_message_multistorage(message)
    assert len(result1) == 2

    result2 = process_message_multistorage_identical_storages(message)
    assert len(result2) == 2

    assert result1 == result2
