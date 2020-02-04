import calendar
from datetime import datetime, timedelta
import uuid

from snuba import settings
from snuba.datasets.factory import enforce_table_writer
from tests.base import BaseEventsTest


class BaseSubscriptionTest(BaseEventsTest):
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
