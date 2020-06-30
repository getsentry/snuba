import calendar
from datetime import datetime, timedelta
import uuid

from snuba import settings
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

        self.write_raw_events(
            [
                {
                    "event_id": uuid.uuid4().hex,
                    "group_id": tick,
                    "primary_hash": uuid.uuid4().hex,
                    "project_id": self.project_id,
                    "message": "a message",
                    "platform": self.platforms[tick % len(self.platforms)],
                    "datetime": (self.base_time + timedelta(minutes=tick)).strftime(
                        settings.PAYLOAD_DATETIME_FORMAT
                    ),
                    "data": {
                        "received": calendar.timegm(
                            (self.base_time + timedelta(minutes=tick)).timetuple()
                        ),
                    },
                    "organization_id": 1,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
                for tick in range(self.minutes)
            ]
        )
