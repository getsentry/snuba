from datetime import timedelta

from snuba.subscriptions.data import Subscription
from snuba.subscriptions.partitioner import DatasetSubscriptionPartitioner
from tests.subscriptions import BaseSubscriptionTest


class TestBuildRequest(BaseSubscriptionTest):
    def test(self):
        subscription = Subscription(
            123, [], [], timedelta(minutes=10), timedelta(minutes=1)
        )
        partitioner = DatasetSubscriptionPartitioner(self.dataset)
        assert partitioner.build_partition_id(subscription) == 18
