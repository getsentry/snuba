from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError

from snuba.consumers.strict_consumer import (
    CommitDecision,
    NoPartitionAssigned,
    StrictConsumer,
)
from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from tests.backends.confluent_kafka import (
    FakeConfluentKafkaConsumer,
    build_confluent_kafka_message,
)


class TestStrictConsumer:
    broker_config = get_default_kafka_configuration(bootstrap_servers=["somewhere"])

    def __consumer(self, on_message) -> StrictConsumer:
        return StrictConsumer(
            topic="my_topic",
            group_id="something",
            broker_config=self.broker_config,
            initial_auto_offset_reset="earliest",
            partition_assignment_timeout=1,
            on_partitions_assigned=None,
            on_partitions_revoked=None,
            on_message=on_message,
        )

    @patch("snuba.consumers.strict_consumer.StrictConsumer._create_consumer")
    def test_empty_topic(self, create_consumer) -> None:
        kafka_consumer = FakeConfluentKafkaConsumer()
        kafka_consumer.items = [
            build_confluent_kafka_message(0, 0, None, True),
        ]
        create_consumer.return_value = kafka_consumer

        on_message = MagicMock()
        consumer = self.__consumer(on_message)

        consumer.run()
        on_message.assert_not_called()

    @patch("snuba.consumers.strict_consumer.StrictConsumer._create_consumer")
    def test_failure(self, create_consumer) -> None:
        kafka_consumer = FakeConfluentKafkaConsumer()
        create_consumer.return_value = kafka_consumer

        on_message = MagicMock()
        consumer = self.__consumer(on_message)

        with pytest.raises(NoPartitionAssigned):
            consumer.run()

        on_message.assert_not_called()

    @patch("snuba.consumers.strict_consumer.StrictConsumer._create_consumer")
    def test_one_message(self, create_consumer) -> None:
        kafka_consumer = FakeConfluentKafkaConsumer()
        create_consumer.return_value = kafka_consumer

        msg = build_confluent_kafka_message(0, 0, b"ABCABC", False)
        kafka_consumer.items = [
            msg,
            build_confluent_kafka_message(0, 0, None, True),
        ]

        on_message = MagicMock()
        on_message.return_value = CommitDecision.DO_NOT_COMMIT
        consumer = self.__consumer(on_message)

        consumer.run()
        on_message.assert_called_once_with(msg)
        assert kafka_consumer.commit_calls == 0

    @patch("snuba.consumers.strict_consumer.StrictConsumer._create_consumer")
    def test_commits(self, create_consumer) -> None:
        kafka_consumer = FakeConfluentKafkaConsumer()
        create_consumer.return_value = kafka_consumer
        error = MagicMock()
        error.code.return_value = KafkaError._PARTITION_EOF
        kafka_consumer.items = [
            build_confluent_kafka_message(0, 0, b"ABCABC", False),
            build_confluent_kafka_message(1, 0, b"ABCABC", False),
            build_confluent_kafka_message(2, 0, b"ABCABC", False),
            build_confluent_kafka_message(0, 0, None, True),
        ]

        on_message = MagicMock()
        on_message.return_value = CommitDecision.COMMIT_PREV
        consumer = self.__consumer(on_message)

        consumer.run()
        on_message.assert_called()
        assert kafka_consumer.commit_calls == 2
