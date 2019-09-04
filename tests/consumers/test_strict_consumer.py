import pytest

from confluent_kafka import KafkaError
from unittest.mock import patch
from unittest.mock import MagicMock


from base import FakeKafkaConsumer, message

from snuba.consumers.strict_consumer import CommitDecision, NoPartitionAssigned, StrictConsumer
from snuba.perf import FakeKafkaMessage


class TestStrictConsumer:

    def __message(self, offset, partition, value, eof=False) -> FakeKafkaMessage:
        if eof:
            error = MagicMock()
            error.code.return_value = KafkaError._PARTITION_EOF
        else:
            error = None

        return FakeKafkaMessage(
            topic="my_topic",
            partition=partition,
            offset=offset,
            value=value,
            key=None,
            headers=None,
            error=error,
        )

    def __consumer(self, on_message) -> StrictConsumer:
        return StrictConsumer(
            topic="my_topic",
            bootstrap_servers="somewhere",
            group_id="something",
            initial_auto_offset_reset="earliest",
            partition_assignment_timeout=1,
            on_partitions_assigned=None,
            on_partitions_revoked=None,
            on_message=on_message,
        )

    @patch('snuba.consumers.strict_consumer.StrictConsumer._create_consumer')
    def test_empty_topic(self, create_consumer) -> None:
        kafka_consumer = FakeKafkaConsumer()
        kafka_consumer.items = [
            message(0, 0, None, True),
        ]
        create_consumer.return_value = kafka_consumer

        on_message = MagicMock()
        consumer = self.__consumer(on_message)

        consumer.run()
        on_message.assert_not_called()

    @patch('snuba.consumers.strict_consumer.StrictConsumer._create_consumer')
    def test_failure(self, create_consumer) -> None:
        kafka_consumer = FakeKafkaConsumer()
        create_consumer.return_value = kafka_consumer

        on_message = MagicMock()
        consumer = self.__consumer(on_message)

        with pytest.raises(NoPartitionAssigned):
            consumer.run()

        on_message.assert_not_called()

    @patch('snuba.consumers.strict_consumer.StrictConsumer._create_consumer')
    def test_one_message(self, create_consumer) -> None:
        kafka_consumer = FakeKafkaConsumer()
        create_consumer.return_value = kafka_consumer

        msg = message(0, 0, "ABCABC", False)
        kafka_consumer.items = [
            msg,
            message(0, 0, None, True),
        ]

        on_message = MagicMock()
        on_message.return_value = CommitDecision.DO_NOT_COMMIT
        consumer = self.__consumer(on_message)

        consumer.run()
        on_message.assert_called_once_with(msg)
        assert kafka_consumer.commit_calls == 0

    @patch('snuba.consumers.strict_consumer.StrictConsumer._create_consumer')
    def test_commits(self, create_consumer) -> None:
        kafka_consumer = FakeKafkaConsumer()
        create_consumer.return_value = kafka_consumer
        error = MagicMock()
        error.code.return_value = KafkaError._PARTITION_EOF
        kafka_consumer.items = [
            message(0, 0, "ABCABC", False),
            message(1, 0, "ABCABC", False),
            message(2, 0, "ABCABC", False),
            message(0, 0, None, True),
        ]

        on_message = MagicMock()
        on_message.return_value = CommitDecision.COMMIT_PREV
        consumer = self.__consumer(on_message)

        consumer.run()
        on_message.assert_called()
        assert kafka_consumer.commit_calls == 2
