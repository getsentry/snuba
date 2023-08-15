import time
import uuid
from contextlib import closing
from datetime import datetime, timedelta
from unittest import mock

import pytest
from arroyo.backends.kafka import KafkaProducer
from arroyo.types import BrokerValue, Message, Partition, Topic
from py._path.local import LocalPath

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.combined_scheduler_executor import (
    CombinedSchedulerExecutorFactory,
)
from snuba.subscriptions.data import PartitionId, SubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.utils import Tick
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic as SnubaTopic
from snuba.utils.types import Interval
from tests.backends.metrics import TestingMetricsBackend

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


def create_subscription() -> None:
    store = RedisSubscriptionDataStore(redis_client, EntityKey.EVENTS, PartitionId(0))
    entity = get_entity(EntityKey.EVENTS)
    store.create(
        uuid.uuid4(),
        SubscriptionData(
            project_id=1,
            time_window_sec=60,
            resolution_sec=60,
            query="MATCH (events) SELECT count()",
            entity=entity,
            metadata={},
        ),
    )


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_combined_scheduler_and_executor(tmpdir: LocalPath) -> None:
    create_subscription()
    epoch = datetime(1970, 1, 1)

    topic = Topic("snuba-commit-log")
    partitions = {Partition(topic, 0): 0}
    partition = Partition(topic, 0)
    commit = mock.Mock()

    producer = KafkaProducer(
        build_kafka_producer_configuration(SnubaTopic.SUBSCRIPTION_RESULTS_EVENTS)
    )

    with closing(producer):
        factory = CombinedSchedulerExecutorFactory(
            dataset=get_dataset("events"),
            entity_names=["events"],
            partitions=1,
            total_concurrent_queries=2,
            producer=producer,
            metrics=TestingMetricsBackend(),
            stale_threshold_seconds=None,
            result_topic="events-subscription-results",
            schedule_ttl=60,
            health_check_file=str(tmpdir / ("health.txt")),
        )

        strategy = factory.create_with_partitions(commit, partitions)

        message = Message(
            BrokerValue(
                Tick(
                    0,
                    offsets=Interval(1, 3),
                    timestamps=Interval(epoch, epoch + timedelta(seconds=60)),
                ),
                partition,
                4,
                epoch,
            )
        )
        strategy.submit(message)

        # Wait for the query to be executed and the result message produced
        for i in range(10):
            time.sleep(0.5)
            strategy.poll()
            if commit.call_count == 1:
                break

        assert (tmpdir / "health.txt").check()
        assert commit.call_count == 1
        strategy.close()
        strategy.join()
