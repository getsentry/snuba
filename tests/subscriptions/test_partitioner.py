from datetime import timedelta

from snuba.subscriptions.data import SubscriptionData
from snuba.subscriptions.partitioner import DatasetSubscriptionDataPartitioner
from tests.subscriptions import BaseSubscriptionTest


class TestBuildRequest(BaseSubscriptionTest):
    def test(self):
        data = SubscriptionData(
            123, [], [], timedelta(minutes=10), timedelta(minutes=1)
        )
        partitioner = DatasetSubscriptionDataPartitioner(self.dataset)
        assert partitioner.build_partition_id(data) == 18
