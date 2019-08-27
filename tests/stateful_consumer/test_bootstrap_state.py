from unittest.mock import patch

from base import FakeKafkaConsumer, message

from snuba.stateful_consumer import StateCompletionEvent
from snuba.consumers.strict_consumer import StrictConsumer
from snuba.stateful_consumer.states.bootstrap import BootstrapState


class TestBootstrapState:

    def __consumer(self, on_message) -> StrictConsumer:
        return StrictConsumer(
            topic="topic",
            bootstrap_servers="somewhere",
            group_id="something",
            auto_offset_reset="earliest",
            partition_assignment_timeout=1,
            on_partitions_assigned=None,
            on_partitions_revoked=None,
            on_message=on_message,
        )

    @patch('snuba.consumers.strict_consumer.StrictConsumer.create_consumer')
    def test_empty_topic(self, create_consumer) -> None:
        kafka_consumer = FakeKafkaConsumer()
        kafka_consumer.items = [
            message(0, 0, None, True),
        ]
        create_consumer.return_value = kafka_consumer

        bootstrap = BootstrapState("cdc_control", "somewhere", "something")

        ret = bootstrap.handle(None)
        assert ret[0] == StateCompletionEvent.NO_SNAPSHOT
        assert kafka_consumer.commit_calls == 0

    @patch('snuba.consumers.strict_consumer.StrictConsumer.create_consumer')
    def test_init_snapshot(self, create_consumer) -> None:
        kafka_consumer = FakeKafkaConsumer()
        kafka_consumer.items = [
            message(
                0,
                0,
                '{"snapshot-id":"abc123", "product":"snuba", "event":"snapshot-init"}',
                False,
            ),
            message(0, 0, None, True),
        ]
        create_consumer.return_value = kafka_consumer

        bootstrap = BootstrapState("cdc_control", "somewhere", "something")

        ret = bootstrap.handle(None)
        assert ret[0] == StateCompletionEvent.SNAPSHOT_INIT_RECEIVED
        assert kafka_consumer.commit_calls == 0

    @patch('snuba.consumers.strict_consumer.StrictConsumer.create_consumer')
    def test_snapshot_loaded(self, create_consumer) -> None:
        kafka_consumer = FakeKafkaConsumer()
        kafka_consumer.items = [
            message(
                0,
                0,
                '{"snapshot-id":"abc123", "product":"somewhere-else", "event":"snapshot-init"}',
                False,
            ),
            message(
                1,
                0,
                '{"snapshot-id":"abc123", "product":"snuba", "event":"snapshot-init"}',
                False,
            ),
            message(
                2,
                0,
                (
                    '{"snapshot-id":"abc123", "event":"snapshot-loaded",'
                    '"datasets": {}, "transaction-info": {"xmin":123, "xmax":124, "xip-list": []}'
                    '}'
                ),
                False,
            ),
            message(0, 0, None, True),
        ]
        create_consumer.return_value = kafka_consumer

        bootstrap = BootstrapState("cdc_control", "somewhere", "something")

        ret = bootstrap.handle(None)
        assert ret[0] == StateCompletionEvent.SNAPSHOT_READY_RECEIVED
        assert kafka_consumer.commit_calls == 2
