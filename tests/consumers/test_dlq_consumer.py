import subprocess
import time

import pytest
import rapidjson
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.types import Topic
from confluent_kafka.admin import AdminClient

from snuba.consumers.dlq import (
    DlqInstruction,
    DlqInstructionStatus,
    DlqReplayPolicy,
    load_instruction,
    store_instruction,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import (
    build_kafka_consumer_configuration,
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
        DlqReplayPolicy.REINSERT_DLQ,
        DlqInstructionStatus.NOT_STARTED,
        StorageKey.QUERYLOG,
        None,
        1,
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
    # XXX: We have to wait long enough for Snuba initialization and an assignment
    # to happen but not so long that ExitAfterNMessages has hit the 10 second timeout
    # and exits.
    time.sleep(5)
    proc.poll()
    # Assert the instruction  changed to in-progress
    loaded_instruction = load_instruction()
    assert loaded_instruction is not None
    assert loaded_instruction.status == DlqInstructionStatus.IN_PROGRESS

    producer.produce(
        Topic(SnubaTopic.DEAD_LETTER_QUERYLOG.value),
        payload=KafkaPayload(
            None, rapidjson.dumps({"message": "invalid-message"}).encode("utf-8"), []
        ),
    ).result()

    producer.close()

    # ExitAfterNMessages raises SIGINT and exits by itself
    proc.wait()

    # The instruction should be cleared now
    assert load_instruction() is None

    # Assert the invalid message got re-inserted
    consumer = KafkaConsumer(
        build_kafka_consumer_configuration(
            SnubaTopic.DEAD_LETTER_QUERYLOG,
            # Using the same consumer group so we pick up after the previous invalid message.
            "dlq-test",
            auto_offset_reset="latest",
            strict_offset_reset=False,
        )
    )

    message = consumer.poll(1.0)

    # Since we picked re-insert DLQ, there should be a new message
    assert message is not None
