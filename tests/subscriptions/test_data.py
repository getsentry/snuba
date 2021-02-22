from datetime import datetime, timedelta

from snuba.subscriptions.data import SubscriptionData
from snuba.web.query import parse_and_run_query
from snuba.utils.metrics.timer import Timer
from tests.subscriptions import BaseSubscriptionTest


class TestBuildRequest(BaseSubscriptionTest):
    def test_conditions(self) -> None:
        subscription = SubscriptionData(
            project_id=self.project_id,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        )
        timer = Timer("test")
        request = subscription.build_request(
            self.dataset, datetime.utcnow(), 100, timer,
        )
        result = parse_and_run_query(self.dataset, request, timer)
        assert result.result["data"][0]["count"] == 10
