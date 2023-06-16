import subprocess
import time

import pytest
from arroyo.backends.kafka import KafkaPayload, KafkaProducer
from arroyo.types import Topic
from confluent_kafka.admin import AdminClient

from snuba.consumers.dlq import (
    DlqInstruction,
    DlqPolicy,
    load_instruction,
    store_instruction,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import (
    build_kafka_producer_configuration,
    get_default_kafka_configuration,
)
from snuba.utils.streams.topics import Topic as SnubaTopic


@pytest.mark.redis_db
def test_dlq_consumer_cli() -> None:
    admin_client = AdminClient(get_default_kafka_configuration())
    create_topics(admin_client, [SnubaTopic.DEAD_LETTER_QUERYLOG])

    producer = KafkaProducer(
        build_kafka_producer_configuration(SnubaTopic.DEAD_LETTER_QUERYLOG)
    )

    instruction = DlqInstruction(
        DlqPolicy.DROP_INVALID_MESSAGES, StorageKey.QUERYLOG, None, 1
    )

    store_instruction(instruction)
    assert load_instruction() == instruction

    proc = subprocess.Popen(
        [
            "snuba",
            "dlq-consumer",
            "--consumer-group",
            "dlq-test",
            "--auto-offset-reset",
            "latest",
        ]
    )
    # We have to wait for Snuba initialization and an assignment to happen
    time.sleep(10)
    proc.poll()
    # Assert the instruction is cleared
    assert load_instruction() is None

    producer.produce(
        Topic(SnubaTopic.DEAD_LETTER_QUERYLOG.value),
        payload=KafkaPayload(None, b"invalid message", []),
    ).result()

    producer.close()

    # ExitAfterNMessages raises SIGINT and exits by itself
    proc.wait()
