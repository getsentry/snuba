import calendar
import uuid
from datetime import datetime, timedelta

import pytest

from snuba import settings
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity, get_entity_name
from snuba.datasets.entity import Entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import InsertEvent
from tests.helpers import write_raw_unprocessed_events, write_unprocessed_events


class BaseSubscriptionTest:
    @pytest.fixture(autouse=True)
    def setup_teardown(self, clickhouse_db: None) -> None:
        self.project_id = 1
        self.platforms = ["a", "b"]
        self.minutes = 20
        self.dataset = get_dataset("events")
        self.entity = get_entity(EntityKey.EVENTS)
        self.entity_key = get_entity_name(self.entity)

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
        ga_storage = get_writable_storage(StorageKey.GROUP_ATTRIBUTES)
        write_raw_unprocessed_events(
            ga_storage,
            [
                {
                    "group_deleted": False,
                    "project_id": self.project_id,
                    "group_id": tick,
                    "status": 0,
                    "substatus": 7,
                    "first_seen": (self.base_time + timedelta(minutes=tick)).strftime(
                        settings.PAYLOAD_DATETIME_FORMAT
                    ),
                    "num_comments": 0,
                    "assignee_user_id": None,
                    "assignee_team_id": None,
                    "owner_suspect_commit_user_id": None,
                    "owner_ownership_rule_user_id": None,
                    "owner_ownership_rule_team_id": None,
                    "owner_codeowners_user_id": None,
                    "owner_codeowners_team_id": None,
                    "timestamp": (self.base_time + timedelta(minutes=tick)).strftime(
                        settings.PAYLOAD_DATETIME_FORMAT
                    ),
                }
                for tick in range(self.minutes)
            ],
        )


def __entity_eq__(self: Entity, other: object) -> bool:
    if not isinstance(other, Entity):
        return False
    return type(self) == type(other)


Entity.__eq__ = __entity_eq__  # type: ignore
