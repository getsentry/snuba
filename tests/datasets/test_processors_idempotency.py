from datetime import datetime
from typing import Tuple

import pytest

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.errors_processor import ErrorsProcessor
from snuba.datasets.processors.transactions_processor import (
    TransactionsMessageProcessor,
)
from snuba.processor import InsertEvent, MessageProcessor
from tests.fixtures import get_raw_error_message, get_raw_transaction_message

test_data = [
    pytest.param(
        get_raw_error_message(),
        ErrorsProcessor(),
        id="errors processor",
    ),
    pytest.param(
        get_raw_transaction_message(),
        TransactionsMessageProcessor(),
        id="transaction processor",
    ),
]


@pytest.mark.parametrize("message,processor", test_data)
def test_processors_of_multistorage_consumer_are_idempotent(
    message: Tuple[int, str, InsertEvent], processor: MessageProcessor
) -> None:
    """
    Test that when the same message is provided to the processors, the result would be the same. That is the process
    message operation is idempotent.
    """
    metadata = KafkaMessageMetadata(1000, 1, datetime.now())

    result1 = processor.process_message(message, metadata)
    result2 = processor.process_message(message, metadata)

    assert result1 == result2
