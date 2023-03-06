from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.functions_processor import FunctionsMessageProcessor
from snuba.processor import InsertBatch


@dataclass
class CallTree:
    depth: int
    fingerprint: str
    name: str
    package: str
    path: str
    is_application: bool
    duration: float
    children: Sequence["CallTree"]

    def serialize(self) -> Mapping[str, Any]:
        serialized = {
            "fingerprint": int(self.fingerprint, 16),
            "name": self.name,
            "package": self.package,
            "is_application": self.is_application,
            "duration_ns": self.duration,
        }

        if self.path:
            serialized["path"] = self.path

        if self.children:
            serialized["children"] = [c.serialize() for c in self.children]

        return serialized


@dataclass
class ProfileCallTreeEvent:
    project_id: int
    profile_id: str
    transaction_name: str
    timestamp: int
    call_trees: Mapping[str, Sequence[CallTree]]
    platform: str
    environment: Optional[str]
    release: Optional[str]
    os_name: str
    os_version: str
    retention_days: int
    received: int

    def serialize(self) -> Mapping[str, Any]:
        return {
            "project_id": self.project_id,
            "profile_id": self.profile_id,
            "transaction_name": self.transaction_name,
            "timestamp": self.timestamp,
            "call_trees": {
                k: [v.serialize() for v in vs] for k, vs in self.call_trees.items()
            },
            "platform": self.platform,
            "environment": self.environment,
            "release": self.release,
            "os_name": self.os_name,
            "os_version": self.os_version,
            "retention_days": self.retention_days,
            "received": self.received,
        }


class TestFunctionsProcessor:
    def test_process_message(self) -> None:
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )

        now = datetime.now(timezone.utc)
        message = ProfileCallTreeEvent(
            project_id=22,
            profile_id="a" * 32,
            timestamp=now.timestamp(),
            received=now.timestamp(),
            transaction_name="vroom-vroom",
            call_trees={
                "259": [
                    CallTree(
                        depth=0,
                        fingerprint="a" * 16,
                        name="foo",
                        package="",
                        path="",
                        is_application=True,
                        duration=10,
                        children=[
                            CallTree(
                                depth=1,
                                fingerprint="c" * 16,
                                name="bar",
                                package="",
                                path="",
                                is_application=False,
                                duration=5,
                                children=[],
                            ),
                        ],
                    ),
                    CallTree(
                        depth=0,
                        fingerprint="b" * 16,
                        name="baz",
                        package="",
                        path="",
                        is_application=True,
                        duration=5,
                        children=[],
                    ),
                    CallTree(
                        depth=0,
                        fingerprint="a" * 16,
                        name="foo",
                        package="",
                        path="",
                        is_application=True,
                        duration=20,
                        children=[
                            CallTree(
                                depth=1,
                                fingerprint="c" * 16,
                                name="bar",
                                package="",
                                path="",
                                is_application=True,
                                duration=10,
                                children=[],
                            ),
                        ],
                    ),
                ],
            },
            platform="cocoa",
            environment="prod",
            release="7.14.0 (1)",
            os_name="iOS",
            os_version="15.2",
            retention_days=30,
        )

        base = {
            "profile_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "project_id": 22,
            "transaction_name": "vroom-vroom",
            "timestamp": now.replace(tzinfo=None),
            "platform": "cocoa",
            "environment": "prod",
            "release": "7.14.0 (1)",
            "os_name": "iOS",
            "os_version": "15.2",
            "retention_days": 30,
            "materialization_version": 0,
        }

        batch = [
            {
                "depth": 0,
                "parent_fingerprint": 0,
                "fingerprint": int("a" * 16, 16),
                "name": "foo",
                "package": "",
                "path": "",
                "is_application": 1,
                "durations": [10, 20],
                **base,
            },
            {
                "depth": 1,
                "parent_fingerprint": int("a" * 16, 16),
                "fingerprint": int("c" * 16, 16),
                "name": "bar",
                "package": "",
                "path": "",
                "is_application": 0,
                "durations": [5, 10],
                **base,
            },
            {
                "depth": 0,
                "parent_fingerprint": 0,
                "fingerprint": int("b" * 16, 16),
                "name": "baz",
                "package": "",
                "path": "",
                "is_application": 1,
                "durations": [5],
                **base,
            },
        ]

        assert FunctionsMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch(batch, datetime.utcfromtimestamp(message.received))

    def test_process_message_without_received(self) -> None:
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )

        now = datetime.now(timezone.utc)
        message = ProfileCallTreeEvent(
            project_id=22,
            profile_id="a" * 32,
            timestamp=now.timestamp(),
            transaction_name="vroom-vroom",
            call_trees={
                "259": [
                    CallTree(
                        depth=0,
                        fingerprint="a" * 16,
                        name="foo",
                        package="",
                        path="",
                        is_application=True,
                        duration=10,
                        children=[],
                    ),
                ],
            },
            platform="cocoa",
            environment="prod",
            release="7.14.0 (1)",
            os_name="iOS",
            os_version="15.2",
            retention_days=30,
            received=None,
        )

        batch = [
            {
                "profile_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "project_id": 22,
                "transaction_name": "vroom-vroom",
                "timestamp": now.replace(tzinfo=None),
                "platform": "cocoa",
                "environment": "prod",
                "release": "7.14.0 (1)",
                "os_name": "iOS",
                "os_version": "15.2",
                "retention_days": 30,
                "materialization_version": 0,
                "depth": 0,
                "parent_fingerprint": 0,
                "fingerprint": int("a" * 16, 16),
                "name": "foo",
                "package": "",
                "path": "",
                "is_application": 1,
                "durations": [10],
            },
        ]

        assert FunctionsMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch(batch, None)
