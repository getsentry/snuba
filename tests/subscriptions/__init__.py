import calendar
import random
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

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

_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
_SERVER_NAME = "D23CXQ4GK2.local"


def gen_span_message(
    dt: datetime,
    tags: dict[str, str] | None = None,
) -> Mapping[str, Any]:
    tags = tags or {}
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "data": {
            "sentry.environment": "development",
            "sentry.release": _RELEASE_TAG,
            "thread.name": "uWSGIWorker1Core0",
            "thread.id": "8522009600",
            "sentry.segment.name": "/api/0/relays/projectconfigs/",
            "sentry.sdk.name": "sentry.python.django",
            "sentry.sdk.version": "2.7.0",
            "my.float.field": 101.2,
            "my.int.field": 2000,
            "my.neg.field": -100,
            "my.neg.float.field": -101.2,
            "my.true.bool.field": True,
            "my.false.bool.field": False,
            "my.numeric.attribute": 1,
        },
        "measurements": {
            "num_of_spans": {"value": 50.0},
            "eap.measurement": {"value": random.choice([1, 100, 1000])},
        },
        "organization_id": 1,
        "origin": "auto.http.django",
        "project_id": 1,
        "received": 1721319572.877828,
        "retention_days": 90,
        "segment_id": "8873a98879faf06d",
        "sentry_tags": {
            "category": "http",
            "environment": "development",
            "op": "http.server",
            "platform": "python",
            "release": _RELEASE_TAG,
            "sdk.name": "sentry.python.django",
            "sdk.version": "2.7.0",
            "status": "ok",
            "status_code": "200",
            "thread.id": "8522009600",
            "thread.name": "uWSGIWorker1Core0",
            "trace.status": "ok",
            "transaction": "/api/0/relays/projectconfigs/",
            "transaction.method": "POST",
            "transaction.op": "http.server",
            "user": "ip:127.0.0.1",
        },
        "span_id": "123456781234567D",
        "tags": {
            "http.status_code": "200",
            "relay_endpoint_version": "3",
            "relay_id": "88888888-4444-4444-8444-cccccccccccc",
            "relay_no_cache": "False",
            "relay_protocol_version": "3",
            "relay_use_post_or_schedule": "True",
            "relay_use_post_or_schedule_rejected": "version",
            "server_name": _SERVER_NAME,
            "spans_over_limit": "False",
            "color": random.choice(["red", "green", "blue"]),
            "location": random.choice(["mobile", "frontend", "backend"]),
            **tags,
        },
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(dt.replace(tzinfo=UTC).timestamp()) * 1000
        - int(random.gauss(1000, 200)),
        "start_timestamp_precise": dt.replace(tzinfo=UTC).timestamp(),
        "end_timestamp_precise": dt.replace(tzinfo=UTC).timestamp() + 1,
    }


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

        items_storage = get_writable_storage(StorageKey("eap_items"))
        messages = [
            gen_span_message(self.base_time + timedelta(minutes=tick))
            for tick in range(self.minutes)
        ]
        extra_messages = [
            gen_span_message(self.base_time - timedelta(hours=4)) for _ in range(2)
        ]
        write_raw_unprocessed_events(items_storage, extra_messages + messages)


def __entity_eq__(self: Entity, other: object) -> bool:
    if not isinstance(other, Entity):
        return False
    return type(self) == type(other)


Entity.__eq__ = __entity_eq__  # type: ignore
