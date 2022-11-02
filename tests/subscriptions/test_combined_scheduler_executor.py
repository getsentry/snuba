import time
import uuid
from contextlib import closing
from datetime import datetime, timedelta
from unittest import mock

from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaProducer

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity_subscriptions.entity_subscription import EventsSubscription
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
    store.create(
        uuid.uuid4(),
        SubscriptionData(
            project_id=1,
            time_window_sec=60,
            resolution_sec=60,
            query="MATCH (events) SELECT count()",
            entity_subscription=EventsSubscription(),
        ),
    )


def test_combined_scheduler_and_executor() -> None:
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
        )

        strategy = factory.create_with_partitions(commit, partitions)

        message = Message(
            partition,
            4,
            Tick(
                0,
                offsets=Interval(1, 3),
                timestamps=Interval(epoch, epoch + timedelta(seconds=60)),
            ),
            epoch,
        )
        strategy.submit(message)

        # Wait for the query to be executed and the result message produced
        for i in range(10):
            time.sleep(0.5)
            strategy.poll()
            if commit.call_count == 1:
                break

        assert commit.call_count == 1
        strategy.close()
        strategy.join()
