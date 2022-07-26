from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.replays_processor import ReplaysProcessor
from snuba.processor import InsertBatch


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
    ipv4: str
    ipv6: str | None
    user_name: str
    user_id: str
    user_email: str
    sdk_name: str
    sdk_version: str
    title: str

    def serialize(self) -> Mapping[Any, Any]:
        return {
            "datetime": "2019-08-08T22:29:53.917000Z",
            "organization_id": 1,
            "platform": self.platform,
            "project_id": 1,
            "replay_id": self.replay_id,
            "message": "/organizations/:orgId/issues/",
            "retention_days": 23,
            "segment_id": self.segment_id,
            "trace_ids": self.trace_ids,
            "data": {
                "replay_id": self.replay_id,
                "environment": self.environment,
                "project_id": 1,
                "release": self.release,
                "dist": self.dist,
                "sdk": {
                    "version": self.sdk_version,
                    "name": self.sdk_name,
                    "packages": [{"version": "0.9.0", "name": "pypi:sentry-sdk"}],
                },
                "platform": self.platform,
                "version": "7",
                "location": "/organizations/:orgId/issues/",
                "type": "replay_event",
                "datetime": datetime.utcfromtimestamp(self.timestamp),
                "timestamp": self.timestamp,
                "tags": [
                    ["sentry:release", self.release],
                    ["sentry:user", self.user_id],
                    ["environment", self.environment],
                    ["we|r=d", "tag"],
                ],
                "user": {
                    "username": self.user_name,
                    "ip_address": self.ipv4 or self.ipv6,
                    "id": self.user_id,
                    "email": self.user_email,
                    # "geo": self.geo,
                },
                "title": self.title,
            },
        }

    def build_result(self, meta: KafkaMessageMetadata) -> Mapping[str, Any]:
        ret = {
            "project_id": 1,
            "replay_id": str(uuid.UUID(self.replay_id)),
            "segment_id": self.segment_id,
            "trace_ids": self.trace_ids,
            "timestamp": datetime.utcfromtimestamp(self.timestamp),
            "platform": self.platform,
            "environment": self.environment,
            "release": self.release,
            "dist": self.dist,
            "user": self.user_id,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "user_email": self.user_email,
            "tags.key": ["environment", "sentry:release", "sentry:user", "we|r=d"],
            "tags.value": [self.environment, self.release, self.user_id, "tag"],
            "sdk_name": "sentry.python",
            "sdk_version": "0.9.0",
            "retention_days": 30,
            "offset": meta.offset,
            "partition": meta.partition,
        }

        if self.ipv4:
            ret["ip_address_v4"] = self.ipv4
        else:
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
