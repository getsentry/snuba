from datetime import timedelta

import pytest

from snuba import settings
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.data import (
    LegacySubscriptionData,
    SnQLSubscriptionData,
    SubscriptionData,
)
from snuba.subscriptions.partitioner import TopicSubscriptionDataPartitioner
from snuba.utils.streams.topics import Topic
from tests.subscriptions import BaseSubscriptionTest

TESTS = [
    pytest.param(
        LegacySubscriptionData(
            project_id=123,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
        ),
        id="Legacy subscription",
    ),
    pytest.param(
        SnQLSubscriptionData(
            project_id=123,
            query=(
                "MATCH (events) "
                "SELECT count() AS count BY time "
                "WHERE "
                "platform IN tuple('a') "
            ),
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
        ),
        id="SnQL subscription",
    ),
]


class TestBuildRequest(BaseSubscriptionTest):
    @pytest.mark.parametrize("subscription", TESTS)
    def test(self, subscription: SubscriptionData) -> None:
        settings.TOPIC_PARTITION_COUNTS = {"events": 64}
        partitioner = TopicSubscriptionDataPartitioner(KafkaTopicSpec(Topic.EVENTS))

        assert partitioner.build_partition_id(subscription) == 18
