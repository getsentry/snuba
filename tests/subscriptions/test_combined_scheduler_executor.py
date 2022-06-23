import time
import uuid
from contextlib import closing
from datetime import datetime, timedelta
from unittest import mock

from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaProducer

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.redis import redis_client
from snuba.subscriptions.combined_scheduler_executor import (
    CombinedSchedulerExecutorFactory,
)
from snuba.subscriptions.data import PartitionId, SubscriptionData
from snuba.subscriptions.entity_subscription import EventsSubscription
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.utils import Tick
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic as SnubaTopic
from snuba.utils.types import Interval
from tests.backends.metrics import TestingMetricsBackend


def create_subscription() -> None:
    store = RedisSubscriptionDataStore(redis_client, EntityKey.EVENTS, PartitionId(0))
    store.create(
        uuid.uuid4(),
        SubscriptionData(
            project_id=1,
            time_window_sec=60,
            resolution_sec=60,
            query="MATCH (events) SELECT count()",
            entity_subscription=EventsSubscription(data_dict={}),
        ),
    )


def test_combined_scheduler_and_executor() -> None:
    state.set_config("subscription_mode_events", "new")
    create_subscription()
    epoch = datetime(1970, 1, 1)

    dataset = get_dataset("events")
    entity_names = ["events"]
    num_partitions = 2
    max_concurrent_queries = 2
    total_concurrent_queries = 2
    metrics = TestingMetricsBackend()

    commit = mock.Mock()
    partitions = mock.Mock()

    topic = Topic("snuba-commit-log")
    partition = Partition(topic, 0)
    stale_threshold_seconds = None
    result_topic = "events-subscription-results"
    schedule_ttl = 60

    producer = KafkaProducer(
        build_kafka_producer_configuration(SnubaTopic.SUBSCRIPTION_RESULTS_EVENTS)
    )

    with closing(producer):
        factory = CombinedSchedulerExecutorFactory(
            dataset,
            entity_names,
            num_partitions,
            max_concurrent_queries,
            total_concurrent_queries,
            producer,
            metrics,
            stale_threshold_seconds,
            result_topic,
            schedule_ttl,
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
