import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, MutableMapping, Optional, Tuple
from uuid import UUID, uuid1

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.utils.clock import TestingClock

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.query.conditions import ConditionFunctions, get_first_level_and_conditions
from snuba.query.matchers import (
    Column,
    FunctionCall,
    Literal,
    MatchResult,
    Pattern,
    String,
)
from snuba.subscriptions.data import (
    PartitionId,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
    SubscriptionTaskResult,
)
from snuba.subscriptions.scheduler import SubscriptionScheduler
from snuba.subscriptions.store import SubscriptionDataStore
from snuba.subscriptions.utils import Tick
from snuba.subscriptions.worker import SubscriptionWorker, handle_differences
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.types import Interval
from tests.backends.metrics import Increment, TestingMetricsBackend
from tests.subscriptions.subscriptions_utils import create_entity_subscription


class DummySubscriptionDataStore(SubscriptionDataStore):
    def __init__(self) -> None:
        self.__subscriptions: MutableMapping[UUID, SubscriptionData] = {}

    def create(self, key: UUID, data: SubscriptionData) -> None:
        self.__subscriptions[key] = data

    def delete(self, key: UUID) -> None:
        try:
            del self.__subscriptions[key]
        except KeyError:
            pass

    def all(self) -> Iterable[Tuple[UUID, SubscriptionData]]:
        return [*self.__subscriptions.items()]


@dataclass(frozen=True)
class Datetime(Pattern[datetime]):
    """
    Matches one specific datetime.
    """

    value: datetime

    def match(self, node: Any) -> Optional[MatchResult]:
        return MatchResult() if node == self.value else None


SUBSCRIPTION_FIXTURES = [
    SubscriptionData(
        project_id=1,
        query=("MATCH (events) SELECT count() AS count"),
        time_window_sec=60 * 60,
        resolution_sec=60,
        entity_subscription=create_entity_subscription(),
    ),
    SubscriptionData(
        project_id=123,
        query=(
            """MATCH (sessions) SELECT if(greater(sessions,0),
                divide(sessions_crashed,sessions),null)
                AS _crash_rate_alert_aggregate, identity(sessions) AS _total_sessions
                WHERE org_id = 1 AND project_id IN tuple(1) LIMIT 1
                OFFSET 0 GRANULARITY 3600"""
        ),
        time_window_sec=10 * 60,
        resolution_sec=60,
        entity_subscription=create_entity_subscription(EntityKey.SESSIONS, 1),
    ),
    SubscriptionData(
        project_id=123,
        query=(
            """MATCH (metrics_counters) SELECT sum(value) AS value BY project_id, tags[3]
                WHERE org_id = 1 AND project_id IN array(1) AND tags[3] IN array(3,4,
                5) AND metric_id=7"""
        ),
        time_window_sec=10 * 60,
        resolution_sec=60,
        entity_subscription=create_entity_subscription(EntityKey.METRICS_COUNTERS, 1),
    ),
    SubscriptionData(
        project_id=123,
        query=(
            """MATCH (metrics_sets) SELECT uniq(value) AS value BY project_id, tags[3]
                WHERE org_id = 1 AND project_id IN array(1) AND tags[3] IN array(3,4,
                5) AND metric_id=7"""
        ),
        time_window_sec=10 * 60,
        resolution_sec=60,
        entity_subscription=create_entity_subscription(EntityKey.METRICS_SETS, 1),
    ),
]

SUBSCRIPTION_WORKER_TEST_CASES = [
    pytest.param(
        SUBSCRIPTION_FIXTURES[0],
        "events",
        EntityKey.EVENTS,
        {"meta": [{"name": "count", "type": "UInt64"}], "data": [{"count": 0}]},
        id="snql",
    ),
    pytest.param(
        SUBSCRIPTION_FIXTURES[1],
        "sessions",
        EntityKey.SESSIONS,
        {
            "meta": [
                {"name": "_crash_rate_alert_aggregate", "type": "Nullable(Float64)"},
                {"type": "UInt64", "name": "_total_sessions"},
            ],
            "data": [{"_crash_rate_alert_aggregate": None, "_total_sessions": 0}],
        },
        id="snql",
    ),
    pytest.param(
        SUBSCRIPTION_FIXTURES[2],
        "metrics",
        EntityKey.METRICS_COUNTERS,
        {
            "data": [],
            "meta": [
                {"name": "project_id", "type": "UInt64"},
                {"name": "tags[3]", "type": "UInt64"},
                {"name": "value", "type": "Float64"},
            ],
        },
        id="snql",
    ),
    pytest.param(
        SUBSCRIPTION_FIXTURES[3],
        "metrics",
        EntityKey.METRICS_SETS,
        {
            "data": [],
            "meta": [
                {"name": "project_id", "type": "UInt64"},
                {"name": "tags[3]", "type": "UInt64"},
                {"name": "value", "type": "UInt64"},
            ],
        },
        id="snql",
    ),
]


@pytest.mark.parametrize(
    "subscription_data, dataset_name, entity_key, expected_result",
    SUBSCRIPTION_WORKER_TEST_CASES,
)
def test_subscription_worker(
    subscription_data: SubscriptionData,
    dataset_name: str,
    entity_key: EntityKey,
    expected_result: MutableMapping[str, Any],
) -> None:

    broker: Broker[SubscriptionTaskResult] = Broker(
        MemoryMessageStorage(), TestingClock()
    )

    result_topic = Topic("subscription-results")

    broker.create_topic(result_topic, partitions=1)

    frequency = timedelta(minutes=1)
    evaluations = 3

    subscription = Subscription(
        SubscriptionIdentifier(PartitionId(0), uuid1()), subscription_data,
    )

    store = DummySubscriptionDataStore()
    store.create(subscription.identifier.uuid, subscription.data)

    metrics = DummyMetricsBackend(strict=True)

    dataset = get_dataset(dataset_name)
    worker = SubscriptionWorker(
        dataset,
        ThreadPoolExecutor(),
        {
            0: SubscriptionScheduler(
                entity_key, store, PartitionId(0), timedelta(), metrics
            )
        },
        broker.get_producer(),
        result_topic,
        metrics,
    )

    now = datetime(2000, 1, 1)

    tick = Tick(
        None,
        offsets=Interval(0, 1),
        timestamps=Interval(now - (frequency * evaluations), now),
    )

    topic = Topic(dataset_name)
    result_futures = worker.process_message(Message(Partition(topic, 0), 0, tick, now))

    assert result_futures is not None and len(result_futures) == evaluations

    # Publish the results.
    worker.flush_batch([result_futures])

    # Check to make sure the results were published.
    # NOTE: This does not cover the ``SubscriptionTaskResultCodec``!
    consumer = broker.get_consumer("group")
    consumer.subscribe([result_topic])

    for i in range(evaluations):
        timestamp = now - frequency * (evaluations - i)

        message = consumer.poll()
        assert message is not None
        assert message.partition.topic == result_topic

        task, future = result_futures[i]
        future_result = request, result = future.result()
        assert message.payload.task.timestamp == timestamp
        assert message.payload == SubscriptionTaskResult(task, future_result)
        del result["profile"]

        timestamp_field = "timestamp" if dataset_name != "sessions" else "started"
        from_pattern = FunctionCall(
            String(ConditionFunctions.GTE),
            (
                Column(None, String(timestamp_field)),
                Literal(
                    Datetime(
                        timestamp - timedelta(seconds=subscription.data.time_window_sec)
                    )
                ),
            ),
        )
        to_pattern = FunctionCall(
            String(ConditionFunctions.LT),
            (Column(None, String(timestamp_field)), Literal(Datetime(timestamp))),
        )

        condition = request.query.get_condition()
        assert condition is not None

        conditions = get_first_level_and_conditions(condition)

        assert any([from_pattern.match(e) for e in conditions])
        assert any([to_pattern.match(e) for e in conditions])

        assert result["data"] == expected_result["data"]
        assert result["meta"] == expected_result["meta"]


def test_subscription_worker_consistent() -> None:
    subscription_data = SUBSCRIPTION_FIXTURES[0]
    state.set_config("event_subscription_non_consistent_sample_rate", 1)
    broker: Broker[SubscriptionTaskResult] = Broker(
        MemoryMessageStorage(), TestingClock()
    )

    result_topic = Topic("subscription-results")

    broker.create_topic(result_topic, partitions=1)

    frequency = timedelta(minutes=1)
    evaluations = 1

    subscription = Subscription(
        SubscriptionIdentifier(PartitionId(0), uuid1()), subscription_data,
    )

    store = DummySubscriptionDataStore()
    store.create(subscription.identifier.uuid, subscription.data)

    metrics = TestingMetricsBackend()

    dataset = get_dataset("events")
    worker = SubscriptionWorker(
        dataset,
        ThreadPoolExecutor(),
        {
            0: SubscriptionScheduler(
                EntityKey.EVENTS,
                store,
                PartitionId(0),
                timedelta(),
                DummyMetricsBackend(strict=True),
            )
        },
        broker.get_producer(),
        result_topic,
        metrics,
    )

    now = datetime(2000, 1, 1)

    tick = Tick(
        None,
        offsets=Interval(0, 1),
        timestamps=Interval(now - (frequency * evaluations), now),
    )

    worker.process_message(Message(Partition(Topic("events"), 0), 0, tick, now))

    time.sleep(0.1)

    assert (
        len(
            [
                m
                for m in metrics.calls
                if isinstance(m, Increment) and m.name == "consistent"
            ]
        )
        == 1
    )


def test_handle_differences() -> None:
    assert handle_differences({"data": [{"a": float("nan"), "b": None}]}) == {
        "data": [{"a": "nan", "b": None}]
    }

    assert handle_differences({"data": [], "profile": {}}) == {"data": []}
