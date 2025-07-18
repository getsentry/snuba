import base64
import importlib
import json
import logging
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Optional
from unittest import mock

import pytest
from arroyo.backends.kafka import KafkaPayload, KafkaProducer
from arroyo.backends.kafka.commit import CommitCodec
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.commit import Commit
from arroyo.errors import ConsumerError
from arroyo.types import BrokerValue, Partition, Topic
from arroyo.utils.clock import MockedClock
from confluent_kafka.admin import AdminClient
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionRequest as CreateSubscriptionRequestProto,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
    Function,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba import settings
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.subscriptions import scheduler_consumer
from snuba.subscriptions.scheduler_consumer import CommitLogTickConsumer
from snuba.subscriptions.types import Interval
from snuba.subscriptions.utils import Tick
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import (
    build_kafka_producer_configuration,
    get_default_kafka_configuration,
)
from snuba.utils.streams.topics import Topic as SnubaTopic
from snuba.web.rpc.v1.create_subscription import CreateSubscriptionRequest
from tests.assertions import assert_changes
from tests.backends.metrics import TestingMetricsBackend

commit_codec = CommitCodec()


@pytest.mark.redis_db
def test_scheduler_consumer(tmpdir: Path) -> None:
    settings.KAFKA_TOPIC_MAP = {
        "events": "events-scheduler-consumer-test",
        "snuba-commit-log": "snuba-commit-log-test",
    }
    importlib.reload(scheduler_consumer)

    admin_client = AdminClient(get_default_kafka_configuration())
    create_topics(admin_client, [SnubaTopic.EVENTS], 2)
    create_topics(admin_client, [SnubaTopic.COMMIT_LOG], 1)

    metrics_backend = TestingMetricsBackend()
    entity_name = "events"
    entity_key = EntityKey(entity_name)
    entity = get_entity(entity_key)
    storage = entity.get_writable_storage()
    assert storage is not None
    stream_loader = storage.get_table_writer().get_stream_loader()

    commit_log_topic = Topic("snuba-commit-log-test")

    mock_scheduler_producer = mock.Mock()

    from snuba.redis import RedisClientKey, get_redis_client
    from snuba.subscriptions.data import PartitionId, SnQLSubscriptionData
    from snuba.subscriptions.store import RedisSubscriptionDataStore

    partition_index = 0

    store = RedisSubscriptionDataStore(
        get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
        entity_key,
        PartitionId(partition_index),
    )
    entity = get_entity(EntityKey.EVENTS)
    store.create(
        uuid.uuid4(),
        SnQLSubscriptionData(
            project_id=1,
            time_window_sec=60,
            resolution_sec=60,
            query="MATCH events SELECT count()",
            entity=entity,
            metadata={},
        ),
    )

    builder = scheduler_consumer.SchedulerBuilder(
        entity_name,
        str(uuid.uuid1().hex),
        "events",
        [],
        mock_scheduler_producer,
        "latest",
        False,
        60 * 5,
        None,
        metrics_backend,
        health_check_file=str(tmpdir / "health.txt"),
    )
    scheduler = builder.build_consumer()
    time.sleep(2)
    scheduler._run_once()
    scheduler._run_once()
    scheduler._run_once()

    epoch = 1000

    producer = KafkaProducer(
        build_kafka_producer_configuration(
            stream_loader.get_default_topic_spec().topic,
        )
    )

    for partition, offset, ts in [
        (0, 0, epoch),
        (1, 0, epoch + 60),
        (0, 1, epoch + 120),
        (1, 1, epoch + 180),
    ]:
        fut = producer.produce(
            commit_log_topic,
            payload=commit_codec.encode(
                Commit(
                    "events",
                    Partition(commit_log_topic, partition),
                    offset,
                    ts,
                    ts,
                )
            ),
        )
        fut.result()

    producer.close()

    for _ in range(5):
        scheduler._run_once()

    scheduler._shutdown()

    assert (tmpdir / "health.txt").exists()
    assert mock_scheduler_producer.produce.call_count == 2

    settings.KAFKA_TOPIC_MAP = {}
    del stream_loader.get_default_topic_spec().partitions_number


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_scheduler_consumer_rpc_subscriptions(tmpdir: Path) -> None:
    metrics_backend = TestingMetricsBackend()
    entity_name = "eap_items"
    entity = get_entity(EntityKey(entity_name))
    storage = entity.get_writable_storage()

    assert storage is not None

    admin_client = AdminClient(get_default_kafka_configuration())

    create_topics(admin_client, [SnubaTopic.ITEMS], 1)
    create_topics(admin_client, [SnubaTopic.ITEMS_COMMIT_LOG], 1)

    stream_loader = storage.get_table_writer().get_stream_loader()
    mock_scheduler_producer = mock.Mock()

    message = CreateSubscriptionRequestProto(
        time_series_request=TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                ),
            ],
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="foo"),
                    op=ComparisonFilter.OP_NOT_EQUALS,
                    value=AttributeValue(val_str="bar"),
                )
            ),
        ),
        time_window_secs=300,
        resolution_secs=60,
    )

    CreateSubscriptionRequest().execute(message)

    followed_consumer_group = "eap-items-consumers"
    builder = scheduler_consumer.SchedulerBuilder(
        entity_name,
        str(uuid.uuid1().hex),
        followed_consumer_group,
        [],
        mock_scheduler_producer,
        "latest",
        False,
        60 * 5,
        None,
        metrics_backend,
        health_check_file=str(tmpdir / "health.txt"),
    )
    scheduler = builder.build_consumer()

    time.sleep(2)

    scheduler._run_once()
    scheduler._run_once()
    scheduler._run_once()

    epoch = 1000

    producer = KafkaProducer(
        build_kafka_producer_configuration(
            stream_loader.get_default_topic_spec().topic,
        )
    )

    for partition, offset, ts in [
        (0, 0, epoch),
        (0, 1, epoch + 120),
    ]:
        fut = producer.produce(
            Topic(SnubaTopic.ITEMS_COMMIT_LOG.value),
            payload=commit_codec.encode(
                Commit(
                    followed_consumer_group,
                    Partition(Topic(SnubaTopic.ITEMS.value), partition),
                    offset,
                    ts,
                    ts,
                )
            ),
        )
        fut.result()

    producer.close()

    for _ in range(5):
        scheduler._run_once()

    scheduler._shutdown()

    assert (tmpdir / "health.txt").exists()
    assert mock_scheduler_producer.produce.call_count == 2
    payload = json.loads(mock_scheduler_producer.produce.call_args_list[0][0][1].value)
    assert payload["task"]["data"]["project_id"] == 1
    assert payload["task"]["data"]["resolution"] == 60
    assert payload["task"]["data"]["time_window"] == 300
    assert payload["task"]["data"]["request_name"] == "TimeSeriesRequest"
    assert payload["task"]["data"]["request_version"] == "v1"
    time_series_request = payload["task"]["data"]["time_series_request"]
    TimeSeriesRequest().ParseFromString(base64.b64decode(time_series_request))


def test_tick_time_shift() -> None:
    partition = 0
    offsets = Interval(0, 1)
    tick = Tick(partition, offsets, Interval(0, 60 * 60 * 24))
    assert tick.time_shift(timedelta(hours=24).total_seconds()) == Tick(
        partition,
        offsets,
        Interval(
            datetime(1970, 1, 2, tzinfo=UTC).timestamp(),
            datetime(1970, 1, 3, tzinfo=UTC).timestamp(),
        ),
    )


@pytest.mark.parametrize(
    "time_shift",
    [
        pytest.param(None, id="without time shift"),
        pytest.param(timedelta(minutes=-5), id="with time shift"),
    ],
)
def test_tick_consumer(time_shift: Optional[timedelta]) -> None:
    clock = MockedClock()
    broker: Broker[KafkaPayload] = Broker(MemoryMessageStorage(), clock)

    epoch = datetime.fromtimestamp(clock.time())

    topic = Topic("messages")
    followed_consumer_group = "events"

    broker.create_topic(topic, partitions=1)

    producer = broker.get_producer()

    for partition, offsets in enumerate([[0, 1, 2], [0]]):
        for offset in offsets:
            payload = commit_codec.encode(
                Commit(
                    followed_consumer_group,
                    Partition(topic, partition),
                    offset,
                    epoch.timestamp(),
                    epoch.timestamp(),
                )
            )
            producer.produce(Partition(topic, 0), payload).result()

    inner_consumer = broker.get_consumer("group")

    consumer = CommitLogTickConsumer(
        inner_consumer,
        followed_consumer_group,
        TestingMetricsBackend(),
        "orig_message_ts",
        time_shift=time_shift,
    )

    if time_shift is None:
        time_shift = timedelta()

    def _assignment_callback(offsets: Mapping[Partition, int]) -> None:
        assert consumer.tell() == {
            Partition(topic, 0): 0,
        }

    assignment_callback = mock.Mock(side_effect=_assignment_callback)

    consumer.subscribe([topic], on_assign=assignment_callback)

    with assert_changes(lambda: assignment_callback.called, False, True):
        # consume 0, 0
        assert consumer.poll() is None

    assert consumer.tell() == {
        Partition(topic, 0): 1,
    }

    # consume 0, 1
    assert consumer.poll() == BrokerValue(
        Tick(
            0,
            offsets=Interval(0, 1),
            timestamps=Interval(epoch.timestamp(), epoch.timestamp()),
        ).time_shift(time_shift.total_seconds()),
        Partition(topic, 0),
        1,
        epoch,
    )

    assert consumer.tell() == {
        Partition(topic, 0): 2,
    }

    # consume 0, 2
    assert consumer.poll() == BrokerValue(
        Tick(
            0,
            offsets=Interval(1, 2),
            timestamps=Interval(epoch.timestamp(), epoch.timestamp()),
        ).time_shift(time_shift.total_seconds()),
        Partition(topic, 0),
        2,
        epoch,
    )

    assert consumer.tell() == {
        Partition(topic, 0): 3,
    }

    # consume 1, 0
    assert consumer.poll() is None

    assert consumer.tell() == {
        Partition(topic, 0): 4,
    }

    # consume no message
    assert consumer.poll() is None

    assert consumer.tell() == {
        Partition(topic, 0): 4,
    }

    consumer.seek({Partition(topic, 0): 1})

    assert consumer.tell() == {
        Partition(topic, 0): 1,
    }

    # consume 0, 1
    assert consumer.poll() is None

    assert consumer.tell() == {
        Partition(topic, 0): 2,
    }

    # consume 0, 2
    assert consumer.poll() == BrokerValue(
        Tick(
            0,
            offsets=Interval(1, 2),
            timestamps=Interval(epoch.timestamp(), epoch.timestamp()),
        ).time_shift(time_shift.total_seconds()),
        Partition(topic, 0),
        2,
        epoch,
    )

    assert consumer.tell() == {
        Partition(topic, 0): 3,
    }

    with pytest.raises(ConsumerError):
        consumer.seek({Partition(topic, -1): 0})


def test_tick_consumer_non_monotonic() -> None:
    clock = MockedClock()
    broker: Broker[KafkaPayload] = Broker(MemoryMessageStorage(), clock)

    epoch = datetime.fromtimestamp(clock.time())

    topic = Topic("messages")
    followed_consumer_group = "events"
    partition = Partition(topic, 0)

    broker.create_topic(topic, partitions=1)

    producer = broker.get_producer()

    inner_consumer = broker.get_consumer("group")

    consumer = CommitLogTickConsumer(
        inner_consumer,
        followed_consumer_group,
        TestingMetricsBackend(),
        "orig_message_ts",
    )

    def _assignment_callback(offsets: Mapping[Partition, int]) -> None:
        assert inner_consumer.tell() == {partition: 0}
        assert consumer.tell() == {partition: 0}

    assignment_callback = mock.Mock(side_effect=_assignment_callback)

    consumer.subscribe([topic], on_assign=assignment_callback)

    producer.produce(
        partition,
        commit_codec.encode(
            Commit(
                followed_consumer_group,
                partition,
                0,
                epoch.timestamp(),
                epoch.timestamp(),
            )
        ),
    ).result()

    clock.sleep(1)

    producer.produce(
        partition,
        commit_codec.encode(
            Commit(
                followed_consumer_group,
                partition,
                1,
                epoch.timestamp() + 1,
                epoch.timestamp() + 1,
            )
        ),
    ).result()

    with assert_changes(lambda: assignment_callback.called, False, True):
        assert consumer.poll() is None

    assert consumer.tell() == {partition: 1}

    with assert_changes(consumer.tell, {partition: 1}, {partition: 2}):
        assert consumer.poll() == BrokerValue(
            Tick(
                0,
                offsets=Interval(0, 1),
                timestamps=Interval(epoch.timestamp(), epoch.timestamp() + 1),
            ),
            partition,
            1,
            epoch + timedelta(seconds=1),
        )

    clock.sleep(-1)

    producer.produce(
        partition,
        commit_codec.encode(
            Commit(
                followed_consumer_group,
                partition,
                2,
                epoch.timestamp(),
                epoch.timestamp(),
            )
        ),
    ).result()

    with assert_changes(consumer.tell, {partition: 2}, {partition: 3}):
        assert consumer.poll() is None

    clock.sleep(2)

    producer.produce(
        partition,
        commit_codec.encode(
            Commit(
                followed_consumer_group,
                partition,
                3,
                epoch.timestamp() + 2,
                epoch.timestamp() + 2,
            )
        ),
    ).result()

    with assert_changes(consumer.tell, {partition: 3}, {partition: 4}):
        assert consumer.poll() == BrokerValue(
            Tick(
                0,
                offsets=Interval(1, 3),
                timestamps=Interval(epoch.timestamp() + 1, epoch.timestamp() + 2),
            ),
            partition,
            3,
            epoch + timedelta(seconds=2),
        )


def test_invalid_commit_log_message(caplog: Any) -> None:
    clock = MockedClock()
    broker: Broker[KafkaPayload] = Broker(MemoryMessageStorage(), clock)

    topic = Topic("messages")
    followed_consumer_group = "events"
    partition = Partition(topic, 0)

    broker.create_topic(topic, partitions=1)

    producer = broker.get_producer()

    inner_consumer = broker.get_consumer("group")

    consumer = CommitLogTickConsumer(
        inner_consumer,
        followed_consumer_group,
        TestingMetricsBackend(),
        "orig_message_ts",
    )

    now = datetime.now()

    def _assignment_callback(offsets: Mapping[Partition, int]) -> None:
        assert inner_consumer.tell() == {partition: 0}
        assert consumer.tell() == {partition: 0}

    assignment_callback = mock.Mock(side_effect=_assignment_callback)

    consumer.subscribe([topic], on_assign=assignment_callback)

    # produce invalid payload to commit log topic (key should not be None)
    producer.produce(
        partition,
        KafkaPayload(None, b"some-value", []),
    ).result()

    clock.sleep(1)

    with caplog.at_level(logging.ERROR):
        assert consumer.poll() is None

    assert followed_consumer_group in caplog.text

    # producing out of order messages to commit log topic does not error
    producer.produce(
        partition,
        commit_codec.encode(
            Commit(
                followed_consumer_group,
                partition,
                5,
                now.timestamp(),
                now.timestamp(),
            )
        ),
    ).result()

    producer.produce(
        partition,
        commit_codec.encode(
            Commit(
                followed_consumer_group,
                partition,
                4,
                now.timestamp() - 2,
                now.timestamp() - 2,
            )
        ),
    ).result()

    consumer.poll()
