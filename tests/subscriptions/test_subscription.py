from datetime import timedelta
from typing import Generator, List, Tuple, cast
from uuid import UUID

import pytest
from pytest import raises

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.redis import redis_client
from snuba.subscriptions.data import SnQLSubscriptionData, SubscriptionData
from snuba.subscriptions.entity_subscription import InvalidSubscriptionError
from snuba.subscriptions.partitioner import TopicSubscriptionDataPartitioner
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.subscription import SubscriptionCreator, SubscriptionDeleter
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException
from tests.subscriptions import BaseSubscriptionTest
from tests.subscriptions.subscriptions_utils import create_entity_subscription

TESTS_CREATE = [
    pytest.param(
        SnQLSubscriptionData(
            project_id=123,
            query=(
                "MATCH (events) "
                "SELECT count() AS count "
                "WHERE "
                "platform IN tuple('a')"
            ),
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
            entity_subscription=create_entity_subscription(),
        ),
        id="SnQL subscription",
    ),
]

TESTS_CREATE_SESSIONS = [
    pytest.param(
        SnQLSubscriptionData(
            project_id=123,
            query=(
                """MATCH (sessions) SELECT if(greater(sessions,0),
                divide(sessions_crashed,sessions),null)
                AS _crash_rate_alert_aggregate, identity(sessions) AS _total_sessions
                WHERE org_id = 1 AND project_id IN tuple(1) LIMIT 1
                OFFSET 0 GRANULARITY 3600"""
            ),
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
            entity_subscription=create_entity_subscription(dataset_name="sessions"),
        ),
        id="Snql subscription",
    ),
]

TESTS_CREATE_METRICS = [
    pytest.param(
        SnQLSubscriptionData(
            project_id=123,
            query=(
                """MATCH (metrics_counters) SELECT sum(value) AS value BY project_id, tags[3]
                WHERE org_id = 1 AND project_id IN array(1) AND metric_id = 7"""
            ),
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
            entity_subscription=create_entity_subscription(dataset_name="metrics"),
        ),
        id="Snql subscription",
    ),
]

TESTS_INVALID = [
    pytest.param(
        SnQLSubscriptionData(
            project_id=123,
            query=(
                "MATCH (events) "
                "SELECT count() AS count "
                "WHERE "
                "platfo IN tuple('a') "
            ),
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
            entity_subscription=create_entity_subscription(),
        ),
        id="SnQL subscription",
    ),
]


class TestSubscriptionCreator(BaseSubscriptionTest):

    timer = Timer("test")

    @pytest.fixture(autouse=True)
    def subscription_rollout(self) -> Generator[None, None, None]:
        state.set_config("snql_subscription_rollout_pct", 1.0)
        yield
        state.set_config("snql_subscription_rollout", 0.0)

    @pytest.mark.parametrize("subscription", TESTS_CREATE)
    def test(self, subscription: SubscriptionData) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        identifier = creator.create(subscription, self.timer)
        assert (
            cast(
                List[Tuple[UUID, SubscriptionData]],
                RedisSubscriptionDataStore(
                    redis_client, self.entity_key, identifier.partition,
                ).all(),
            )[0][1]
            == subscription
        )

    @pytest.mark.parametrize("subscription", TESTS_INVALID)
    def test_invalid_condition_column(self, subscription: SubscriptionData) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        with raises(QueryException):
            creator.create(
                SnQLSubscriptionData(
                    project_id=123,
                    resolution=timedelta(minutes=1),
                    time_window=timedelta(minutes=10),
                    query="MATCH (events) SELECT count() AS count WHERE platfo IN tuple('a')",
                    entity_subscription=create_entity_subscription(),
                ),
                self.timer,
            )

    def test_invalid_aggregation(self) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        with raises(QueryException):
            creator.create(
                SnQLSubscriptionData(
                    project_id=123,
                    resolution=timedelta(minutes=1),
                    time_window=timedelta(minutes=10),
                    query="MATCH (events) SELECT cout() AS count WHERE platform IN tuple('a')",
                    entity_subscription=create_entity_subscription(),
                ),
                self.timer,
            )

    def test_invalid_time_window(self) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        with raises(InvalidSubscriptionError):
            creator.create(
                SnQLSubscriptionData(
                    project_id=123,
                    resolution=timedelta(minutes=1),
                    time_window=timedelta(),
                    query="MATCH (events) SELECT count() AS count WHERE platfo IN tuple('a')",
                    entity_subscription=create_entity_subscription(),
                ),
                self.timer,
            )

        with raises(InvalidSubscriptionError):
            creator.create(
                SnQLSubscriptionData(
                    project_id=123,
                    query=(
                        "MATCH (events) "
                        "SELECT count() AS count BY time "
                        "WHERE "
                        "platform IN tuple('a') "
                    ),
                    time_window=timedelta(minutes=1),
                    resolution=timedelta(),
                    entity_subscription=create_entity_subscription(),
                ),
                self.timer,
            )

        with raises(InvalidSubscriptionError):
            creator.create(
                SnQLSubscriptionData(
                    project_id=123,
                    resolution=timedelta(minutes=1),
                    time_window=timedelta(hours=48),
                    query="MATCH (events) SELECT count() AS count WHERE platfo IN tuple('a')",
                    entity_subscription=create_entity_subscription(),
                ),
                self.timer,
            )

    def test_invalid_resolution(self) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        with raises(InvalidSubscriptionError):
            creator.create(
                SnQLSubscriptionData(
                    project_id=123,
                    resolution=timedelta(),
                    time_window=timedelta(minutes=1),
                    query="MATCH (events) SELECT count() AS count WHERE platfo IN tuple('a')",
                    entity_subscription=create_entity_subscription(),
                ),
                self.timer,
            )


class TestSessionsSubscriptionCreator:
    timer = Timer("test")

    @pytest.mark.parametrize("subscription", TESTS_CREATE_SESSIONS)
    def test(self, subscription: SubscriptionData) -> None:
        dataset = get_dataset("sessions")
        creator = SubscriptionCreator(dataset, EntityKey.SESSIONS)
        identifier = creator.create(subscription, self.timer)
        assert (
            cast(
                List[Tuple[UUID, SubscriptionData]],
                RedisSubscriptionDataStore(
                    redis_client, EntityKey.SESSIONS, identifier.partition,
                ).all(),
            )[0][1]
            == subscription
        )


class TestMetricsCountersSubscriptionCreator:
    timer = Timer("test")

    @pytest.mark.parametrize("subscription", TESTS_CREATE_METRICS)
    def test(self, subscription: SubscriptionData) -> None:
        dataset = get_dataset("metrics")
        creator = SubscriptionCreator(dataset)
        # XXX (ahmed): hack to circumvent using the default entity of a dataset as the default
        # entity for the metrics dataset is METRICS_SETS, and this subscription type is currently
        # not supported. Will add a fix shortly that relies on passing the entity key rather
        # than relying on fetching the default entity from a dataset
        creator.entity_key = EntityKey.METRICS_COUNTERS
        writable_storage = get_entity(creator.entity_key).get_writable_storage()
        assert writable_storage is not None
        creator.__partitioner = TopicSubscriptionDataPartitioner(
            writable_storage.get_table_writer()
            .get_stream_loader()
            .get_default_topic_spec()
        )
        identifier = creator.create(subscription, self.timer)
        assert (
            cast(
                List[Tuple[UUID, SubscriptionData]],
                RedisSubscriptionDataStore(
                    redis_client, EntityKey.METRICS_COUNTERS, identifier.partition,
                ).all(),
            )[0][1]
            == subscription
        )


class TestSubscriptionDeleter(BaseSubscriptionTest):
    def test(self) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        subscription = SnQLSubscriptionData(
            project_id=1,
            query="MATCH (events) SELECT count() AS count",
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
            entity_subscription=create_entity_subscription(),
        )
        identifier = creator.create(subscription, Timer("test"))
        assert (
            cast(
                List[Tuple[UUID, SubscriptionData]],
                RedisSubscriptionDataStore(
                    redis_client, self.entity_key, identifier.partition,
                ).all(),
            )[0][1]
            == subscription
        )

        SubscriptionDeleter(self.entity_key, identifier.partition).delete(
            identifier.uuid
        )
        assert (
            RedisSubscriptionDataStore(
                redis_client, self.entity_key, identifier.partition,
            ).all()
            == []
        )
