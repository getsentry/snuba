from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import md5
from typing import Any, Mapping

import pytest

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.replays_processor import (
    ReplaysProcessor,
    coerce_list,
    coerce_uuid,
    maybe,
    stringify,
)
from snuba.processor import InsertBatch
from snuba.util import force_bytes


@dataclass
class ReplayEvent:
    replay_id: str
    segment_id: int
    trace_ids: Any
    error_ids: Any
    urls: Any
    is_archived: int | None
    timestamp: float
    replay_start_timestamp: float | None
    platform: Any
    environment: Any
    release: Any
    dist: Any
    ipv4: str | None
    ipv6: str | None
    user_name: Any
    user_id: Any
    user_email: Any
    os_name: Any
    os_version: Any
    browser_name: Any
    browser_version: Any
    device_name: Any
    device_brand: Any
    device_family: Any
    device_model: Any
    sdk_name: Any
    sdk_version: Any
    title: str | None

    def serialize(self) -> Mapping[Any, Any]:
        replay_event = {
            "type": "replay_event",
            "replay_id": self.replay_id,
            "segment_id": self.segment_id,
            "tags": {"customtag": "is_set", "transaction": self.title},
            "urls": self.urls,
            "is_archived": self.is_archived,
            "trace_ids": self.trace_ids,
            "error_ids": self.error_ids,
            "dist": self.dist,
            "platform": self.platform,
            "timestamp": self.timestamp,
            "replay_start_timestamp": self.replay_start_timestamp,
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
                },
                "os": {
                    "name": self.os_name,
                    "version": self.os_version,
                },
                "browser": {
                    "name": self.browser_name,
                    "version": self.browser_version,
                },
                "device": {
                    "name": self.device_name,
                    "brand": self.device_brand,
                    "family": self.device_family,
                    "model": self.device_model,
                },
            },
            "request": {
                "url": "Doesn't matter not ingested.",
                "headers": {"User-Agent": "not used"},
            },
            "extra": {},
        }

        return {
            "type": "replay_event",
            "start_time": self.timestamp,
            "replay_id": self.replay_id,
            "project_id": 1,
            "retention_days": 30,
            "payload": list(bytes(json.dumps(replay_event).encode())),
        }

    def _user_field(self) -> Any | None:
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
            "trace_ids": [
                coerce_uuid(t) for t in coerce_list("trace_ids", self.trace_ids)
            ],
            "error_ids": [
                coerce_uuid(e) for e in coerce_list("error_ids", self.error_ids)
            ],
            "timestamp": datetime.utcfromtimestamp(self.timestamp),
            "replay_start_timestamp": datetime.utcfromtimestamp(
                self.replay_start_timestamp
            )
            if self.replay_start_timestamp
            else None,
            "platform": self.platform,
            "environment": self.environment,
            "release": self.release,
            "dist": self.dist,
            "urls": coerce_list("urls", self.urls),
            "is_archived": 1 if self.is_archived is True else None,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "user_email": self.user_email,
            "os_name": self.os_name,
            "os_version": self.os_version,
            "browser_name": self.browser_name,
            "browser_version": self.browser_version,
            "device_name": self.device_name,
            "device_brand": self.device_brand,
            "device_family": self.device_family,
            "device_model": self.device_model,
            "tags.key": ["customtag"],
            "tags.value": ["is_set"],
            "title": self.title,
            "sdk_name": "sentry.python",
            "sdk_version": "0.9.0",
            "retention_days": 30,
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
            error_ids=["36e980a9c6024cde9f5d089f15b83b5f"],
            trace_ids=[
                "36e980a9c6024cde9f5d089f15b83b5f",
                "8bea4461d8b944f393c15a3cb1c4169a",
            ],
            segment_id=0,
            timestamp=datetime.now(tz=timezone.utc).timestamp(),
            replay_start_timestamp=datetime.now(tz=timezone.utc).timestamp(),
            platform="python",
            dist="",
            urls=["http://localhost:8001"],
            is_archived=True,
            user_name="me",
            user_id="232",
            user_email="test@test.com",
            os_name="iOS",
            os_version="16.2",
            browser_name="Chrome",
            browser_version="103.0.38",
            device_name="iPhone 11",
            device_brand="Apple",
            device_family="iPhone",
            device_model="iPhone",
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

    def test_process_message_mismatched_types(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        message = ReplayEvent(
            replay_id="e5e062bf2e1d4afd96fd2f90b6770431",
            title="/organizations/:orgId/issues/",
            error_ids=["36e980a9c6024cde9f5d089f15b83b5f"],
            trace_ids=[
                "36e980a9c6024cde9f5d089f15b83b5f",
                "8bea4461d8b944f393c15a3cb1c4169a",
            ],
            segment_id=0,
            timestamp=datetime.now(tz=timezone.utc).timestamp(),
            replay_start_timestamp=datetime.now(tz=timezone.utc).timestamp(),
            platform=0,
            dist=0,
            urls=["http://localhost:8001", None, 0],
            is_archived=True,
            user_name=0,
            user_id=0,
            user_email=0,
            os_name=0,
            os_version=0,
            browser_name=0,
            browser_version=0,
            device_name=0,
            device_brand=0,
            device_family=0,
            device_model=0,
            ipv4="127.0.0.1",
            ipv6=None,
            environment=0,
            release=0,
            sdk_name=0,
            sdk_version=0,
        )

        processed_message = ReplaysProcessor().process_message(
            message.serialize(), meta
        )
        assert isinstance(processed_message, InsertBatch)
        assert processed_message.rows[0]["urls"] == ["http://localhost:8001", "0"]
        assert processed_message.rows[0]["platform"] == "0"
        assert processed_message.rows[0]["dist"] == "0"
        assert processed_message.rows[0]["user_name"] == "0"
        assert processed_message.rows[0]["user_id"] == "0"
        assert processed_message.rows[0]["user_email"] == "0"
        assert processed_message.rows[0]["os_name"] == "0"
        assert processed_message.rows[0]["os_version"] == "0"
        assert processed_message.rows[0]["browser_name"] == "0"
        assert processed_message.rows[0]["browser_version"] == "0"
        assert processed_message.rows[0]["device_name"] == "0"
        assert processed_message.rows[0]["device_brand"] == "0"
        assert processed_message.rows[0]["device_family"] == "0"
        assert processed_message.rows[0]["device_model"] == "0"
        assert processed_message.rows[0]["environment"] == "0"
        assert processed_message.rows[0]["release"] == "0"
        assert processed_message.rows[0]["sdk_name"] == "0"
        assert processed_message.rows[0]["sdk_version"] == "0"

    def test_process_message_nulls(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        message = ReplayEvent(
            replay_id="e5e062bf2e1d4afd96fd2f90b6770431",
            title=None,
            error_ids=None,
            trace_ids=None,
            segment_id=0,
            timestamp=datetime.now(tz=timezone.utc).timestamp(),
            replay_start_timestamp=None,
            platform=None,
            dist="",
            urls=None,
            is_archived=None,
            os_name=None,
            os_version=None,
            browser_name=None,
            browser_version=None,
            device_name=None,
            device_brand=None,
            device_family=None,
            device_model=None,
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

    def test_maybe(self) -> None:
        """Test maybe utility function."""

        def identity(a: Any) -> Any:
            return a

        assert maybe(identity, None) is None
        assert maybe(identity, False) is False
        assert maybe(identity, True) is True
        assert maybe(identity, 0) == 0
        assert maybe(identity, "hello") == "hello"

    def test_stringify(self) -> None:
        """Test stringify utility function."""
        assert stringify(None) == ""
        assert stringify(True) == "true"
        assert stringify([0, 1]) == "[0,1]"
        assert stringify("hello") == "hello"
        assert stringify({"hello": "world"}) == '{"hello":"world"}'

    def test_coerce_list(self) -> None:
        """Test "coerce_list" function."""
        assert coerce_list("t", [1, 2]) == [1, 2]
        assert coerce_list("t", "a") == ["a"]
        assert coerce_list("t", None) == []

    def test_coerce_uuid(self) -> None:
        """Test "coerce_uuid" function."""
        uid = uuid.uuid4()
        assert coerce_uuid(uid.hex) == str(uid)

        with pytest.raises(ValueError):
            coerce_uuid("4")
