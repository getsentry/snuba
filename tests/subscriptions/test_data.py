from datetime import datetime, timedelta
from unittest.mock import Mock

from snuba.api.query import parse_and_run_query
from snuba.subscriptions.data import Subscription
from tests.subscriptions import BaseSubscriptionTest


class TestBuildRequest(BaseSubscriptionTest):
    def test_conditions(self):
        subscription = Subscription(
            id="hello",
            project_id=self.project_id,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        )
        request = subscription.build_request(
            self.dataset, datetime.utcnow(), 100, Mock()
        )
        result = parse_and_run_query(self.dataset, request, Mock())
        assert result["data"][0]["count"] == 10
