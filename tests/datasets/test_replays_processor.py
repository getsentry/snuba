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
    sequence_id: int
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
            "event_id": self.replay_id,
            "message": "/organizations/:orgId/issues/",
            "group_id": None,
            "retention_days": 23,
            "data": {
                "event_id": self.replay_id,
                "environment": self.environment,
                "project_id": 1,
                "release": self.release,
                "dist": self.dist,
                "grouping_config": {
                    "enhancements": "eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC",
                    "id": "legacy:2019-03-12",
                },
                "sdk": {
                    "version": self.sdk_version,
                    "name": self.sdk_name,
                    "packages": [{"version": "0.9.0", "name": "pypi:sentry-sdk"}],
                },
                "breadcrumbs": {
                    "values": [
                        {
                            "category": "query",
                            "timestamp": 1565308204.544,
                            "message": "[Filtered]",
                            "type": "default",
                            "level": "info",
                        },
                    ],
                },
                "platform": self.platform,
                "version": "7",
                "location": "/organizations/:orgId/issues/",
                "logger": "",
                "type": "replay_event",
                "metadata": {
                    "location": "/organizations/:orgId/issues/",
                    "title": "/organizations/:orgId/issues/",
                },
                "primary_hash": "d41d8cd98f00b204e9800998ecf8427e",
                "datetime": datetime.utcfromtimestamp(self.timestamp),
                "timestamp": self.timestamp,
                "start_timestamp": self.timestamp,
                "measurements": {
                    "lcp": {"value": 32.129},
                    "lcp.elementSize": {"value": 4242},
                    "fid": {"value": None},
                    "invalid": None,
                    "invalid2": {},
                },
                "breakdowns": {
                    "span_ops": {
                        "ops.db": {"value": 62.512},
                        "ops.http": {"value": 109.774},
                        "total.time": {"value": 172.286},
                    }
                },
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
            trace_ids=[],
            sequence_id=0,
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

        assert (
            message.build_result(meta)
            == ReplaysProcessor().process_message(message.serialize(), meta)[0][0]
        )

        assert ReplaysProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch([message.build_result(meta)], None)
