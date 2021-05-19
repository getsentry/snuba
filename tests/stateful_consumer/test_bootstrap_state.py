from unittest.mock import Mock, patch

from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_cdc_storage
from snuba.stateful_consumer import ConsumerStateCompletionEvent
from snuba.stateful_consumer.states.bootstrap import BootstrapState
from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from tests.backends.confluent_kafka import (
    FakeConfluentKafkaConsumer,
    build_confluent_kafka_message,
)


class TestBootstrapState:
    broker_config = get_default_kafka_configuration(bootstrap_servers=["somewhere"])

    @patch("snuba.consumers.strict_consumer.StrictConsumer._create_consumer")
    def test_empty_topic(self, create_consumer: Mock) -> None:
        kafka_consumer = FakeConfluentKafkaConsumer()
        kafka_consumer.items = [
            build_confluent_kafka_message(0, 0, None, True),
        ]
        create_consumer.return_value = kafka_consumer

        bootstrap = BootstrapState(
            "cdc_control",
            self.broker_config,
            "something",
            get_cdc_storage(StorageKey.GROUPEDMESSAGES),
        )

        ret = bootstrap.handle(None)
        assert ret[0] == ConsumerStateCompletionEvent.NO_SNAPSHOT
        assert kafka_consumer.commit_calls == 0

    @patch("snuba.consumers.strict_consumer.StrictConsumer._create_consumer")
    def test_snapshot_for_other_table(self, create_consumer: Mock) -> None:
        kafka_consumer = FakeConfluentKafkaConsumer()
        kafka_consumer.items = [
            build_confluent_kafka_message(
                0,
                0,
                b'{"snapshot-id":"abc123", "tables": ["someone_else"], "product":"snuba", "event":"snapshot-init"}',
                False,
            ),
            build_confluent_kafka_message(0, 0, None, True),
        ]
        create_consumer.return_value = kafka_consumer

        bootstrap = BootstrapState(
            "cdc_control",
            self.broker_config,
            "something",
            get_cdc_storage(StorageKey.GROUPEDMESSAGES),
        )

        ret = bootstrap.handle(None)
        assert ret[0] == ConsumerStateCompletionEvent.NO_SNAPSHOT
        assert kafka_consumer.commit_calls == 1

    @patch("snuba.consumers.strict_consumer.StrictConsumer._create_consumer")
    def test_init_snapshot(self, create_consumer: Mock) -> None:
        kafka_consumer = FakeConfluentKafkaConsumer()
        kafka_consumer.items = [
            build_confluent_kafka_message(
                0,
                0,
                b'{"snapshot-id":"abc123", "tables": ["sentry_groupedmessage"], "product":"snuba", "event":"snapshot-init"}',
                False,
            ),
            build_confluent_kafka_message(0, 0, None, True),
        ]
        create_consumer.return_value = kafka_consumer

        bootstrap = BootstrapState(
            "cdc_control",
            self.broker_config,
            "something",
            get_cdc_storage(StorageKey.GROUPEDMESSAGES),
        )

        ret = bootstrap.handle(None)
        assert ret[0] == ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED
        assert kafka_consumer.commit_calls == 0

    @patch("snuba.consumers.strict_consumer.StrictConsumer._create_consumer")
    def test_snapshot_loaded(self, create_consumer: Mock) -> None:
        kafka_consumer = FakeConfluentKafkaConsumer()
        kafka_consumer.items = [
            build_confluent_kafka_message(
                0,
                0,
                b'{"snapshot-id":"abc123", "product":"somewhere-else", "tables": [], "event":"snapshot-init"}',
                False,
            ),
            build_confluent_kafka_message(
                1,
                0,
                b'{"snapshot-id":"abc123", "product":"snuba", "tables": ["sentry_groupedmessage"], "event":"snapshot-init"}',
                False,
            ),
            build_confluent_kafka_message(
                2,
                0,
                (
                    b'{"snapshot-id":"abc123", "event":"snapshot-loaded",'
                    b'"transaction-info": {"xmin":123, "xmax":124, "xip-list": []}'
                    b"}"
                ),
                False,
            ),
            build_confluent_kafka_message(0, 0, None, True),
        ]
        create_consumer.return_value = kafka_consumer

        bootstrap = BootstrapState(
            "cdc_control",
            self.broker_config,
            "something",
            get_cdc_storage(StorageKey.GROUPEDMESSAGES),
        )

        ret = bootstrap.handle(None)
        assert ret[0] == ConsumerStateCompletionEvent.SNAPSHOT_READY_RECEIVED
        assert kafka_consumer.commit_calls == 2
