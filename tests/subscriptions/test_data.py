import calendar
from datetime import datetime, timedelta
from unittest.mock import Mock
import uuid

from snuba import settings
from snuba.api.query import parse_and_run_query
from snuba.datasets.factory import enforce_table_writer
from snuba.subscriptions.data import Subscription
from tests.base import BaseEventsTest


class TestApi(BaseEventsTest):
    def setup_method(self, test_method, dataset_name="events"):
        super().setup_method(test_method, dataset_name)
        self.project_id = 1
        self.platforms = ["a", "b"]
        self.minutes = 20

        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=self.minutes)
        self.generate_events()

    def generate_events(self):
        events = []
        for tick in range(self.minutes):
            # project N sends an event every Nth minute
            events.append(
                enforce_table_writer(self.dataset)
                .get_stream_loader()
                .get_processor()
                .process_insert(
                    {
                        "project_id": self.project_id,
                        "event_id": uuid.uuid4().hex,
                        "deleted": 0,
                        "datetime": (self.base_time + timedelta(minutes=tick)).strftime(
                            "%Y-%m-%dT%H:%M:%S.%fZ"
                        ),
                        "message": "a message",
                        "platform": self.platforms[tick % len(self.platforms)],
                        "primary_hash": uuid.uuid4().hex,
                        "group_id": tick,
                        "retention_days": settings.DEFAULT_RETENTION_DAYS,
                        "data": {
                            "received": calendar.timegm(
                                (self.base_time + timedelta(minutes=tick)).timetuple()
                            ),
                        },
                    }
                )
            )
        self.write_processed_records(events)

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
