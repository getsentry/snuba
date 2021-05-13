import calendar
import uuid
from datetime import datetime, timedelta

from snuba import settings
from snuba.datasets.events_processor_base import InsertEvent
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.helpers import write_unprocessed_events


class BaseSubscriptionTest:
    def setup_method(self) -> None:
        self.project_id = 1
        self.platforms = ["a", "b"]
        self.minutes = 20
        self.dataset = get_dataset("events")

        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=self.minutes)

        events_storage = get_writable_storage(StorageKey.ERRORS)

        write_unprocessed_events(
            events_storage,
            [
                InsertEvent(
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
                )
                for tick in range(self.minutes)
            ],
        )
