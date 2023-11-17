from typing import List, Tuple, cast
from uuid import UUID

import pytest
from pytest import raises

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity_subscriptions.validators import InvalidSubscriptionError
from snuba.datasets.factory import get_dataset
from snuba.query.exceptions import InvalidQueryException
from snuba.query.parser.exceptions import ParsingException
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import SubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.subscription import SubscriptionCreator, SubscriptionDeleter
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException
from tests.subscriptions import BaseSubscriptionTest

TESTS_CREATE = [
    pytest.param(
        SubscriptionData(
            project_id=123,
            query=(
                "MATCH (events) "
                "SELECT count() AS count "
                "WHERE "
                "platform IN tuple('a')"
            ),
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        id="SnQL subscription",
    ),
]

TESTS_INVALID = [
    pytest.param(
        SubscriptionData(
            project_id=123,
            query=(
                "MATCH (events) "
                "SELECT count() AS count "
                "WHERE "
                "platfo IN tuple('a') "
            ),
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        id="SnQL subscription",
    ),
]


class TestSubscriptionCreator(BaseSubscriptionTest):
    timer = Timer("test")

    @pytest.mark.parametrize("subscription", TESTS_CREATE)
    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    def test(self, subscription: SubscriptionData) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        identifier = creator.create(subscription, self.timer)
        assert (
            cast(
                List[Tuple[UUID, SubscriptionData]],
                RedisSubscriptionDataStore(
                    get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
                    self.entity_key,
                    identifier.partition,
                ).all(),
            )[0][1]
            == subscription
        )

    @pytest.mark.parametrize("subscription", TESTS_INVALID)
    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    def test_invalid_condition_column(self, subscription: SubscriptionData) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        with raises(ParsingException):
            creator.create(
                subscription,
                self.timer,
            )

    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    def test_invalid_aggregation(self) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        with raises(QueryException):
            creator.create(
                SubscriptionData(
                    project_id=123,
                    time_window_sec=10 * 60,
                    resolution_sec=60,
                    query="MATCH (events) SELECT cout() AS count WHERE platform IN tuple('a')",
                    entity=get_entity(EntityKey.EVENTS),
                    metadata={},
                ),
                self.timer,
            )

    @pytest.mark.clickhouse_db
    def test_invalid_time_window(self) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        with raises(InvalidSubscriptionError):
            creator.create(
                SubscriptionData(
                    project_id=123,
                    time_window_sec=0,
                    resolution_sec=60,
                    query="MATCH (events) SELECT count() AS count WHERE platfo IN tuple('a')",
                    entity=get_entity(EntityKey.EVENTS),
                    metadata={},
                ),
                self.timer,
            )

        with raises(InvalidSubscriptionError):
            creator.create(
                SubscriptionData(
                    project_id=123,
                    query=(
                        "MATCH (events) "
                        "SELECT count() AS count BY time "
                        "WHERE "
                        "platform IN tuple('a') "
                    ),
                    time_window_sec=0,
                    resolution_sec=60,
                    entity=get_entity(EntityKey.EVENTS),
                    metadata={},
                ),
                self.timer,
            )

        with raises(InvalidSubscriptionError):
            creator.create(
                SubscriptionData(
                    project_id=123,
                    time_window_sec=48 * 60 * 60,
                    resolution_sec=60,
                    query="MATCH (events) SELECT count() AS count WHERE platfo IN tuple('a')",
                    entity=get_entity(EntityKey.EVENTS),
                    metadata={},
                ),
                self.timer,
            )

    @pytest.mark.clickhouse_db
    def test_invalid_resolution(self) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        with raises(InvalidSubscriptionError):
            creator.create(
                SubscriptionData(
                    project_id=123,
                    time_window_sec=0,
                    resolution_sec=60,
                    query="MATCH (events) SELECT count() AS count WHERE platfo IN tuple('a')",
                    entity=get_entity(EntityKey.EVENTS),
                    metadata={},
                ),
                self.timer,
            )


TESTS_CREATE_METRICS = [
    pytest.param(
        SubscriptionData(
            project_id=123,
            query="MATCH (metrics_counters) SELECT sum(value) AS value BY project_id, tags[3] WHERE org_id = 1 AND project_id IN array(1) AND metric_id = 7 AND tags[3] IN array(6,7)",
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.METRICS_COUNTERS),
            metadata={"organization": 1},
        ),
        EntityKey.METRICS_COUNTERS,
        id="Metrics Counters Snql subscription",
    ),
    pytest.param(
        SubscriptionData(
            project_id=123,
            query="MATCH (metrics_sets) SELECT uniq(value) AS value BY project_id, tags[3] WHERE org_id = 1 AND project_id IN array(1) AND metric_id = 7 AND tags[3] IN array(6,7)",
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.METRICS_SETS),
            metadata={"organization": 1},
        ),
        EntityKey.METRICS_SETS,
        id="Metrics Sets Snql subscription",
    ),
]


TESTS_INVALID_METRICS = [
    pytest.param(
        SubscriptionData(
            project_id=123,
            query="MATCH (metrics_counters) SELECT sum(value) AS value BY project_id, tags[3] WHERE org_id = 1 AND project_id IN array(1) AND metric_id = 7",
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.METRICS_COUNTERS),
            metadata={"organization": 1},
        ),
        id="Metrics Counters subscription missing tags[3] condition",
    ),
    pytest.param(
        SubscriptionData(
            project_id=123,
            query="MATCH (metrics_counters) SELECT sum(value) AS value BY project_id, tags[3] WHERE org_id = 1 AND metric_id = 7 AND tags[3] IN array(6,7)",
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.METRICS_COUNTERS),
            metadata={"organization": 1},
        ),
        id="Metrics Counters subscription missing project_id condition",
    ),
    pytest.param(
        SubscriptionData(
            project_id=123,
            query="MATCH (metrics_sets) SELECT uniq(value) AS value BY project_id, tags[3] WHERE org_id = 1 AND project_id IN array(1) AND metric_id = 7",
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.METRICS_SETS),
            metadata={"organization": 1},
        ),
        id="Metrics Sets subscription missing tags[3] condition",
    ),
    pytest.param(
        SubscriptionData(
            project_id=123,
            query="MATCH (metrics_sets) SELECT uniq(value) AS value BY project_id, tags[3] WHERE org_id = 1 AND metric_id = 7 AND tags[3] IN array(6,7)",
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.METRICS_SETS),
            metadata={"organization": 1},
        ),
        id="Metrics Sets subscription missing project_id condition",
    ),
]


class TestMetricsCountersSubscriptionCreator:
    timer = Timer("test")

    def setup_method(self) -> None:
        self.dataset = get_dataset("metrics")

    @pytest.mark.parametrize("subscription, entity_key", TESTS_CREATE_METRICS)
    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    def test(self, subscription: SubscriptionData, entity_key: EntityKey) -> None:
        creator = SubscriptionCreator(self.dataset, entity_key)
        identifier = creator.create(subscription, self.timer)
        assert (
            cast(
                List[Tuple[UUID, SubscriptionData]],
                RedisSubscriptionDataStore(
                    get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
                    entity_key,
                    identifier.partition,
                ).all(),
            )[0][1]
            == subscription
        )

    @pytest.mark.parametrize("subscription", TESTS_INVALID_METRICS)
    @pytest.mark.clickhouse_db
    def test_missing_conditions_for_groupby_clause(
        self, subscription: SubscriptionData
    ) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.METRICS_COUNTERS)
        with raises(InvalidQueryException):
            creator.create(
                subscription,
                self.timer,
            )


class TestSubscriptionDeleter(BaseSubscriptionTest):
    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    def test(self) -> None:
        creator = SubscriptionCreator(self.dataset, EntityKey.EVENTS)
        subscription = SubscriptionData(
            project_id=1,
            query="MATCH (events) SELECT count() AS count",
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        )
        identifier = creator.create(subscription, Timer("test"))
        assert (
            cast(
                List[Tuple[UUID, SubscriptionData]],
                RedisSubscriptionDataStore(
                    get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
                    self.entity_key,
                    identifier.partition,
                ).all(),
            )[0][1]
            == subscription
        )

        SubscriptionDeleter(self.entity_key, identifier.partition).delete(
            identifier.uuid
        )
        assert (
            RedisSubscriptionDataStore(
                get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
                self.entity_key,
                identifier.partition,
            ).all()
            == []
        )
