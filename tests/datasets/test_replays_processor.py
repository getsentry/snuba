from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

import pytest

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.replays_processor import (
    ReplaysProcessor,
    maybe,
    normalize_tags,
    process_tags_object,
    raise_on_null,
    segment_id_to_event_hash,
    to_capped_list,
    to_capped_string,
    to_datetime,
    to_enum,
    to_string,
    to_typed_list,
    to_uint16,
    to_uuid,
)
from snuba.processor import InsertBatch

LOG_LEVELS = ["fatal", "error", "warning", "info", "debug"]


@dataclass
class ReplayEvent:
    replay_id: str
    replay_type: str | None
    event_hash: str | None
    error_sample_rate: float | None
    session_sample_rate: float | None
    segment_id: Any
    trace_ids: Any
    error_ids: Any
    urls: Any
    is_archived: int | None
    timestamp: Any
    replay_start_timestamp: Any
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

    @classmethod
    def empty_set(cls) -> ReplayEvent:
        return cls(
            replay_id="e5e062bf2e1d4afd96fd2f90b6770431",
            timestamp=int(datetime.now(timezone.utc).timestamp()),
            segment_id=None,
            replay_type=None,
            event_hash=None,
            error_sample_rate=None,
            session_sample_rate=None,
            title=None,
            error_ids=[],
            trace_ids=[],
            replay_start_timestamp=None,
            platform=None,
            dist=None,
            urls=[],
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
            environment=None,
            release=None,
            sdk_name=None,
            sdk_version=None,
        )

    def serialize(
        self, header_overrides: Optional[dict[Any, Any]] = None
    ) -> Mapping[Any, Any]:
        replay_event: Any = {
            "type": "replay_event",
            "replay_id": self.replay_id,
            "replay_type": self.replay_type,
            "segment_id": self.segment_id,
            "event_hash": self.event_hash,
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
                "replay": {
                    "error_sample_rate": self.error_sample_rate,
                    "session_sample_rate": self.session_sample_rate,
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

        headers = {
            "start_time": datetime.now().timestamp(),
            "type": "replay_event",
            "replay_id": self.replay_id,
            "project_id": 1,
            "retention_days": 30,
        }
        headers.update(header_overrides or {})
        return {
            **headers,
            **{"payload": list(bytes(json.dumps(replay_event).encode()))},
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
        event_hash = segment_id_to_event_hash(self.segment_id)

        ret = {
            "project_id": 1,
            "replay_id": str(uuid.UUID(self.replay_id)),
            "replay_type": self.replay_type,
            "error_sample_rate": self.error_sample_rate,
            "session_sample_rate": self.session_sample_rate,
            "event_hash": self.event_hash or event_hash,
            "segment_id": self.segment_id,
            "trace_ids": list(
                map(to_uuid, to_capped_list("trace_ids", self.trace_ids))
            ),
            "error_ids": list(
                map(to_uuid, to_capped_list("trace_ids", self.error_ids))
            ),
            "timestamp": maybe(int, self.timestamp),
            "replay_start_timestamp": maybe(int, self.replay_start_timestamp),
            "platform": self.platform,
            "environment": self.environment,
            "release": self.release,
            "dist": self.dist,
            "urls": to_capped_list("urls", self.urls),
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

        header_overrides = {
            "start_time": int(datetime.now(tz=timezone.utc).timestamp())
        }
        message = ReplayEvent(
            replay_id="e5e062bf2e1d4afd96fd2f90b6770431",
            replay_type="session",
            event_hash=None,
            error_sample_rate=0.5,
            session_sample_rate=0.5,
            title="/organizations/:orgId/issues/",
            error_ids=["36e980a9c6024cde9f5d089f15b83b5f"],
            trace_ids=[
                "36e980a9c6024cde9f5d089f15b83b5f",
                "8bea4461d8b944f393c15a3cb1c4169a",
            ],
            segment_id=0,
            timestamp=int(datetime.now(tz=timezone.utc).timestamp()),
            replay_start_timestamp=int(datetime.now(tz=timezone.utc).timestamp()),
            platform="python",
            dist="",
            urls=["http://127.0.0.1:8001"],
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
            message.serialize(header_overrides), meta
        ) == InsertBatch(
            [message.build_result(meta)],
            datetime.utcfromtimestamp(header_overrides["start_time"]),
        )

    def test_process_message_mismatched_types(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        now = datetime.now(tz=timezone.utc).replace(microsecond=0).timestamp()

        message = ReplayEvent(
            replay_id="e5e062bf2e1d4afd96fd2f90b6770431",
            replay_type="other",
            event_hash=None,
            error_sample_rate=None,
            session_sample_rate=None,
            title="/organizations/:orgId/issues/",
            error_ids=["36e980a9c6024cde9f5d089f15b83b5f"],
            trace_ids=[
                "36e980a9c6024cde9f5d089f15b83b5f",
                "8bea4461d8b944f393c15a3cb1c4169a",
            ],
            segment_id=0,
            timestamp=str(int(now)),
            replay_start_timestamp=str(int(now)),
            platform=0,
            dist=0,
            urls=["http://127.0.0.1:8001", None, 0],
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
        assert processed_message.rows[0]["urls"] == ["http://127.0.0.1:8001", "0"]
        assert processed_message.rows[0]["replay_type"] == ""
        assert processed_message.rows[0]["error_sample_rate"] == -1.0
        assert processed_message.rows[0]["session_sample_rate"] == -1.0
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
        assert processed_message.rows[0]["timestamp"] == now
        assert processed_message.rows[0]["replay_start_timestamp"] == now

    def test_process_message_minimal_payload_segment_id(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        minimal_payload = {
            "type": "replay_event",
            "start_time": datetime.now().timestamp(),
            "replay_id": str(uuid.uuid4()),
            "project_id": 1,
            "retention_days": 30,
            "payload": list(
                bytes(
                    json.dumps(
                        {
                            "type": "replay_event",
                            "replay_id": str(uuid.uuid4()),
                            "segment_id": 0,
                            "platform": "internal",
                        }
                    ).encode()
                )
            ),
        }

        # Asserting that the minimal payload was successfully processed.
        result = ReplaysProcessor().process_message(minimal_payload, meta)
        assert isinstance(result, InsertBatch)
        assert len(result.rows) == 1
        assert len(result.rows[0]["event_hash"]) == 36

    def test_process_message_minimal_payload_event_hash(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        event_hash = uuid.uuid4().hex
        minimal_payload = {
            "type": "replay_event",
            "start_time": datetime.now().timestamp(),
            "replay_id": str(uuid.uuid4()),
            "project_id": 1,
            "retention_days": 30,
            "payload": list(
                bytes(
                    json.dumps(
                        {
                            "type": "replay_event",
                            "replay_id": str(uuid.uuid4()),
                            "event_hash": event_hash,
                            "platform": "internal",
                        }
                    ).encode()
                )
            ),
        }

        # Asserting that the minimal payload was successfully processed.
        result = ReplaysProcessor().process_message(minimal_payload, meta)
        assert isinstance(result, InsertBatch)
        assert len(result.rows) == 1
        assert result.rows[0]["event_hash"] == str(uuid.UUID(event_hash))

    def test_process_message_nulls(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        message = ReplayEvent.empty_set()

        processed_batch = ReplaysProcessor().process_message(message.serialize(), meta)
        assert isinstance(processed_batch, InsertBatch)  # required for type checker
        received = processed_batch.rows[0]
        assert isinstance(received, dict)  # required for type checker
        received_event_hash = received.pop("event_hash")

        expected = message.build_result(meta)
        assert isinstance(expected, dict)  # required for type checker
        assert received_event_hash != expected.pop("event_hash")  # hash is random

        # Sample rates default to -1.0 which is an impossible state for the field.
        assert received["error_sample_rate"] == -1.0
        assert received["session_sample_rate"] == -1.0

        assert received["replay_type"] == ""
        assert received["platform"] == ""
        assert received["dist"] == ""
        assert received["user_name"] == ""
        assert received["user_id"] == ""
        assert received["user_email"] == ""
        assert received["os_name"] == ""
        assert received["os_version"] == ""
        assert received["browser_name"] == ""
        assert received["browser_version"] == ""
        assert received["device_name"] == ""
        assert received["device_brand"] == ""
        assert received["device_family"] == ""
        assert received["device_model"] == ""
        assert received["environment"] == ""
        assert received["release"] == ""
        assert received["sdk_name"] == ""
        assert received["sdk_version"] == ""
        assert received["replay_start_timestamp"] is None

    def test_process_message_invalid_segment_id(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        message = ReplayEvent.empty_set()

        with pytest.raises(ValueError):
            message.segment_id = "a"
            ReplaysProcessor().process_message(message.serialize(), meta)

        with pytest.raises(ValueError):
            message.segment_id = -1
            ReplaysProcessor().process_message(message.serialize(), meta)

        with pytest.raises(ValueError):
            message.segment_id = 2**16
            ReplaysProcessor().process_message(message.serialize(), meta)

        message.segment_id = 2**16 - 1
        ReplaysProcessor().process_message(message.serialize(), meta)

    def test_process_message_invalid_timestamp(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        message = ReplayEvent.empty_set()

        with pytest.raises(ValueError):
            message.timestamp = "a"
            ReplaysProcessor().process_message(message.serialize(), meta)

        with pytest.raises(ValueError):
            message.timestamp = -1
            ReplaysProcessor().process_message(message.serialize(), meta)

        with pytest.raises(ValueError):
            message.timestamp = 2**32
            ReplaysProcessor().process_message(message.serialize(), meta)

        message.timestamp = 2**32 - 1
        ReplaysProcessor().process_message(message.serialize(), meta)

        message.timestamp = f"{2**32 - 1}"
        ReplaysProcessor().process_message(message.serialize(), meta)

    def test_process_message_invalid_replay_start_timestamp(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        message = ReplayEvent.empty_set()

        with pytest.raises(ValueError):
            message.replay_start_timestamp = "a"
            ReplaysProcessor().process_message(message.serialize(), meta)

        with pytest.raises(ValueError):
            message.replay_start_timestamp = -1
            ReplaysProcessor().process_message(message.serialize(), meta)

        with pytest.raises(ValueError):
            message.replay_start_timestamp = 2**32
            ReplaysProcessor().process_message(message.serialize(), meta)

        message.replay_start_timestamp = 2**32 - 1
        ReplaysProcessor().process_message(message.serialize(), meta)

        message.replay_start_timestamp = f"{2**32 - 1}"
        ReplaysProcessor().process_message(message.serialize(), meta)

    def test_to_uint16(self) -> None:
        """Test "to_uint16" function."""
        assert to_uint16(0) == 0
        assert to_uint16(65535) == 65535
        assert to_uint16(1.25) == 1
        assert to_uint16("1") == 1

        with pytest.raises(ValueError):
            to_uint16(65536)
        with pytest.raises(ValueError):
            to_uint16(-1)
        with pytest.raises(TypeError):
            to_uint16([1])

    def test_to_datetime(self) -> None:
        """Test "to_datetime" function."""
        now = int(datetime.now(timezone.utc).timestamp())
        assert to_datetime(now).timestamp() == now
        assert to_datetime(str(now)).timestamp() == now

        with pytest.raises(ValueError):
            to_datetime(2**32)
        with pytest.raises(ValueError):
            to_datetime(-1)
        with pytest.raises(ValueError):
            to_datetime("a")

    def test_maybe(self) -> None:
        """Test maybe utility function."""

        def identity(a: Any) -> Any:
            return a

        assert maybe(identity, None) is None
        assert maybe(identity, False) is False
        assert maybe(identity, True) is True
        assert maybe(identity, 0) == 0
        assert maybe(identity, "hello") == "hello"

    def test_to_string(self) -> None:
        """Test to_string utility function."""
        assert to_string(None) == ""
        assert to_string(True) == "true"
        assert to_string([0, 1]) == "[0,1]"
        assert to_string("hello") == "hello"
        assert to_string({"hello": "world"}) == '{"hello":"world"}'

    def test_to_capped_list(self) -> None:
        """Test "to_capped_list" function."""
        assert to_capped_list("t", [1, 2]) == [1, 2]
        assert len(to_capped_list("t", [1] * 10_000)) == 1000
        assert to_capped_list("t", None) == []

    def test_to_typed_list(self) -> None:
        """Test "to_typed_list" function."""
        assert to_typed_list(to_uint16, [1, 2, None]) == [1, 2]
        assert to_typed_list(to_string, ["a", 0, None]) == ["a", "0"]

        with pytest.raises(ValueError):
            assert to_typed_list(to_uint16, ["a"])

    def test_to_enum(self) -> None:
        """Test "to_enum" function."""
        assert to_enum(["session", "error"])("session") == "session"
        assert to_enum(["session", "error"])("error") == "error"
        assert to_enum(["session", "error"])("other") is None
        assert to_enum(["session", "error"])(None) is None

    def test_to_uuid(self) -> None:
        """Test "to_uuid" function."""
        uid = uuid.uuid4()
        assert to_uuid(uid.hex) == str(uid)

        with pytest.raises(ValueError):
            to_uuid("4")

    def test_process_tags_object(self) -> None:
        """Test "process_tags_object" function."""

        # Dictionary.

        tags = process_tags_object({"transaction": "/", "hello": "world"})
        assert tags.transaction == "/"
        assert tags.keys == ["hello"]
        assert tags.values == ["world"]

        tags = process_tags_object({"transaction": "/", "hello": None})
        assert tags.transaction == "/"
        assert tags.keys == []
        assert tags.values == []

        tags = process_tags_object({"hello": "world"})
        assert tags.transaction is None
        assert tags.keys == ["hello"]
        assert tags.values == ["world"]

        # Tuple list.

        tags = process_tags_object([("transaction", "/"), ("hello", "world")])
        assert tags.transaction == "/"
        assert tags.keys == ["hello"]
        assert tags.values == ["world"]

        tags = process_tags_object([("transaction", "/"), ("hello", None)])
        assert tags.transaction == "/"
        assert tags.keys == []
        assert tags.values == []

        tags = process_tags_object([("hello", "world")])
        assert tags.transaction is None
        assert tags.keys == ["hello"]
        assert tags.values == ["world"]

        tags = process_tags_object([("hello", "world", "!")])
        assert tags.transaction is None
        assert tags.keys == []
        assert tags.values == []

        # Empty

        tags = process_tags_object(None)
        assert tags.transaction is None
        assert tags.keys == []
        assert tags.values == []

        # Invalid types

        with pytest.raises(TypeError):
            process_tags_object("hello")
        with pytest.raises(TypeError):
            process_tags_object(1)

    def test_normalize_tags(self) -> None:
        """Test "normalize_tags" function."""
        assert normalize_tags([("hello", "world")]) == [("hello", "world")]
        assert normalize_tags({"hello": "world"}) == [("hello", "world")]
        assert normalize_tags([("hello", "world", "!")]) == []

        with pytest.raises(TypeError):
            normalize_tags(1)

        with pytest.raises(TypeError):
            normalize_tags(None)

        with pytest.raises(TypeError):
            normalize_tags("a")

    def test_to_capped_string(self) -> None:
        """Test "to_capped_string" function."""
        assert to_capped_string(1, "123") == "1"
        assert to_capped_string(2, "123") == "12"
        assert to_capped_string(3, "123") == "123"
        assert to_capped_string(4, "123") == "123"

    def test_raise_on_null(self) -> None:
        with pytest.raises(ValueError):
            raise_on_null("test", None)


class TestReplaysActionProcessor:
    def test_replay_actions(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        now = datetime.now(tz=timezone.utc).replace(microsecond=0)

        message = {
            "type": "replay_event",
            "start_time": datetime.now().timestamp(),
            "replay_id": "bb570198b8f04f8bbe87077668530da7",
            "project_id": 1,
            "retention_days": 30,
            "payload": list(
                bytes(
                    json.dumps(
                        {
                            "type": "replay_actions",
                            "replay_id": "bb570198b8f04f8bbe87077668530da7",
                            "clicks": [
                                {
                                    "node_id": 59,
                                    "tag": "div",
                                    "id": "id",
                                    "class": ["class1", "class2"],
                                    "role": "button",
                                    "aria_label": "test",
                                    "alt": "",
                                    "testid": "",
                                    "title": "",
                                    "text": "text",
                                    "timestamp": int(now.timestamp()),
                                    "event_hash": "df3c3aa2daae465e89f1169e49139827",
                                    "is_dead": 0,
                                    "is_rage": 1,
                                }
                            ],
                        }
                    ).encode()
                )
            ),
        }

        result = ReplaysProcessor().process_message(message, meta)
        assert isinstance(result, InsertBatch)
        rows = result.rows
        assert len(rows) == 1

        row = rows[0]
        assert row["project_id"] == 1
        assert row["timestamp"] == now
        assert row["replay_id"] == str(uuid.UUID("bb570198b8f04f8bbe87077668530da7"))
        assert row["event_hash"] == "df3c3aa2daae465e89f1169e49139827"
        assert row["segment_id"] is None
        assert row["trace_ids"] == []
        assert row["error_ids"] == []
        assert row["urls"] == []
        assert row["platform"] == "javascript"
        assert row["user"] is None
        assert row["sdk_name"] is None
        assert row["sdk_version"] is None
        assert row["retention_days"] == 30
        assert row["partition"] == 0
        assert row["offset"] == 0
        assert row["click_node_id"] == 59
        assert row["click_tag"] == "div"
        assert row["click_id"] == "id"
        assert row["click_class"] == ["class1", "class2"]
        assert row["click_aria_label"] == "test"
        assert row["click_role"] == "button"
        assert row["click_text"] == "text"
        assert row["click_alt"] == ""
        assert row["click_testid"] == ""
        assert row["click_title"] == ""
        assert row["click_is_dead"] == 0
        assert row["click_is_rage"] == 1


from hashlib import md5


def make_payload_for_event_link(severity: str) -> tuple[dict[str, Any], str, str]:
    now = datetime.now(tz=timezone.utc).replace(microsecond=0)
    replay_id = "bb570198b8f04f8bbe87077668530da7"
    event_id = uuid.uuid4().hex
    message = {
        "type": "replay_event",
        "start_time": datetime.now().timestamp(),
        "replay_id": replay_id,
        "project_id": 1,
        "retention_days": 30,
        "payload": list(
            bytes(
                json.dumps(
                    {
                        "type": "event_link",
                        "replay_id": replay_id,
                        severity + "_id": event_id,
                        "timestamp": str(int(now.timestamp())),
                        "event_hash": md5(
                            (replay_id + event_id).encode("utf-8")
                        ).hexdigest(),
                    }
                ).encode()
            )
        ),
    }
    return (message, event_id, severity)


@pytest.mark.parametrize(
    "event_link_message",
    [pytest.param(make_payload_for_event_link(log_level)) for log_level in LOG_LEVELS],
)
def test_replay_event_links(
    event_link_message: tuple[dict[str, Any], str, str]
) -> None:
    message, event_id, severity = event_link_message

    meta = KafkaMessageMetadata(offset=0, partition=0, timestamp=datetime(1970, 1, 1))

    result = ReplaysProcessor().process_message(message, meta)
    assert isinstance(result, InsertBatch)
    rows = result.rows
    assert len(rows) == 1

    row = rows[0]
    assert row["project_id"] == 1
    assert "timestamp" in row
    assert row[severity + "_id"] == str(uuid.UUID(event_id))
    assert row["replay_id"] == str(uuid.UUID(message["replay_id"]))
    assert (
        row["event_hash"]
        == md5((message["replay_id"] + event_id).encode("utf-8")).hexdigest()
    )
    assert row["segment_id"] is None
    assert row["retention_days"] == 30
    assert row["partition"] == 0
    assert row["offset"] == 0


@pytest.mark.parametrize(
    "event_link_message",
    [pytest.param(make_payload_for_event_link("not_valid"))],
)
def test_replay_event_links_invalid_severity(
    event_link_message: tuple[dict[str, Any], str, str]
) -> None:
    message, _, _ = event_link_message

    meta = KafkaMessageMetadata(offset=0, partition=0, timestamp=datetime(1970, 1, 1))

    with pytest.raises(ValueError):
        ReplaysProcessor().process_message(message, meta)
