from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import md5
from typing import Any, Mapping

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.replays_processor import ReplaysProcessor
from snuba.processor import InsertBatch
from snuba.util import force_bytes


@dataclass
class ReplayEvent:
    replay_id: str
    segment_id: int
    trace_ids: list[str]
    timestamp: float
    platform: str
    environment: str
    release: str
    dist: str
    url: str | None
    ipv4: str | None
    ipv6: str | None
    user_name: str | None
    user_id: str | None
    user_email: str | None
    sdk_name: str
    sdk_version: str
    title: str | None

    def serialize(self) -> Mapping[Any, Any]:
        return {
            "type": "replay_event",
            "start_time": self.timestamp,
            "replay_id": self.replay_id,
            "project_id": 1,
            "retention_days": 30,
            "payload": list(
                bytes(
                    json.dumps(
                        {
                            "type": "replay_event",
                            "replay_id": self.replay_id,
                            "segment_id": self.segment_id,
                            "tags": {"customtag": "is_set", "transaction": self.title},
                            "trace_ids": self.trace_ids,
                            "dist": self.dist,
                            "platform": self.platform,
                            "timestamp": self.timestamp,
                            "environment": self.environment,
                            "release": self.release,
                            "user": {
                                "id": self.user_id,
                                "username": self.user_name,
                                "email": self.user_email,
                                "ip_address": self.ipv4,
                            },
                            "sdk": {
                                "name": self.sdk_name,
                                "version": self.sdk_version,
                            },
                            "contexts": {
                                "trace": {
                                    "op": "pageload",
                                    "span_id": "affa5649681a1eeb",
                                    "trace_id": "23eda6cd4b174ef8a51f0096df3bfdd1",
                                }
                            },
                            "request": {
                                "url": self.url,
                                "headers": {
                                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
                                },
                            },
                            "extra": {},
                        }
                    ).encode()
                )
            ),
        }

    def _user_field(self) -> str | None:
        user_fields = [
            self.user_id,
            self.user_name,
            self.user_email,
            self.ipv4,
            self.ipv6,
        ]
        for f in user_fields:
            if f is not None:
                return f
        return None

    def build_result(self, meta: KafkaMessageMetadata) -> Mapping[str, Any]:

        segment_id_bytes = force_bytes(str((self.segment_id)))
        segment_hash = md5(segment_id_bytes).hexdigest()
        event_hash = str(uuid.UUID(segment_hash))

        ret = {
            "project_id": 1,
            "replay_id": str(uuid.UUID(self.replay_id)),
            "event_hash": event_hash,
            "segment_id": self.segment_id,
            "trace_ids": self.trace_ids,
            "timestamp": datetime.utcfromtimestamp(self.timestamp),
            "platform": self.platform,
            "environment": self.environment,
            "release": self.release,
            "dist": self.dist,
            "url": self.url,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "user_email": self.user_email,
            "tags.key": ["customtag"],
            "tags.value": ["is_set"],
            "title": self.title,
            "sdk_name": "sentry.python",
            "sdk_version": "0.9.0",
            "retention_days": 30,
            # "url": "http://localhost:3000/", # commented out until we have the url field
            "offset": meta.offset,
            "partition": meta.partition,
        }
        user = self._user_field()

        if user:
            ret["user"] = user

        if self.ipv4:
            ret["ip_address_v4"] = self.ipv4
        elif self.ipv6:
            ret["ip_address_v6"] = self.ipv6
        return ret


class TestReplaysProcessor:
    def test_process_message(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        message = ReplayEvent(
            replay_id="e5e062bf2e1d4afd96fd2f90b6770431",
            title="/organizations/:orgId/issues/",
            trace_ids=[
                "36e980a9-c602-4cde-9f5d-089f15b83b5f",
                "8bea4461-d8b9-44f3-93c1-5a3cb1c4169a",
            ],
            segment_id=0,
            timestamp=datetime.now(tz=timezone.utc).timestamp(),
            platform="python",
            dist="",
            url="http://localhost:8001",
            user_name="me",
            user_id="232",
            user_email="test@test.com",
            ipv4="127.0.0.1",
            ipv6=None,
            environment="prod",
            release="34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a",
            sdk_name="sentry.python",
            sdk_version="0.9.0",
        )
        assert ReplaysProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch([message.build_result(meta)], None)

    def test_process_message_nulls(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        message = ReplayEvent(
            replay_id="e5e062bf2e1d4afd96fd2f90b6770431",
            title=None,
            trace_ids=[],
            segment_id=0,
            timestamp=datetime.now(tz=timezone.utc).timestamp(),
            platform="python",
            dist="",
            url=None,
            user_name=None,
            user_id=None,
            user_email=None,
            ipv4=None,
            ipv6=None,
            environment="prod",
            release="34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a",
            sdk_name="sentry.python",
            sdk_version="0.9.0",
        )

        assert ReplaysProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch([message.build_result(meta)], None)
