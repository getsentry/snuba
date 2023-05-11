from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

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
    timestamp: float
    call_trees: Mapping[str, Sequence[CallTree]]
    platform: str
    environment: Optional[str]
    release: Optional[str]
    os_name: str
    os_version: str
    retention_days: int
    received: Optional[float]

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


@dataclass
class Function:
    fingerprint: int
    function: str
    module: str
    package: str
    in_app: bool
    self_times_ns: Sequence[int]

    def serialize(self) -> Mapping[str, Any]:
        return {
            "fingerprint": self.fingerprint,
            "function": self.function,
            "module": self.module,
            "package": self.package,
            "in_app": self.in_app,
            "self_times_ns": self.self_times_ns,
        }


@dataclass
class ProfileFunctionsEvent:
    project_id: int
    profile_id: str
    transaction_name: str
    timestamp: float
    functions: Sequence[Function]
    platform: str
    environment: Optional[str]
    release: Optional[str]
    dist: Optional[str]
    transaction_op: Optional[str]
    transaction_status: Optional[str]
    http_method: Optional[str]
    browser_name: Optional[str]
    device_class: Optional[int]
    retention_days: int
    received: Optional[float]

    def serialize(self) -> Mapping[str, Any]:
        return {
            "project_id": self.project_id,
            "profile_id": self.profile_id,
            "transaction_name": self.transaction_name,
            "timestamp": self.timestamp,
            "functions": [f.serialize() for f in self.functions],
            "platform": self.platform,
            "environment": self.environment,
            "release": self.release,
            "dist": self.dist,
            "transaction_op": self.transaction_op,
            "transaction_status": self.transaction_status,
            "http_method": self.http_method,
            "browser_name": self.browser_name,
            "device_class": self.device_class,
            "retention_days": self.retention_days,
            "received": self.received,
        }


class TestFunctionsProcessor:
    def test_process_message_call_tree(self) -> None:
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

    def test_process_message_call_tree_without_received(self) -> None:
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

    def test_process_message_functions(self) -> None:
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )

        now = datetime.now(timezone.utc)
        message = ProfileFunctionsEvent(
            project_id=22,
            profile_id="a" * 32,
            transaction_name="vroom-vroom",
            timestamp=now.timestamp(),
            received=now.timestamp(),
            functions=[
                Function(123, "foo", "", "bar", True, [1, 2, 3]),
                Function(456, "baz", "qux", "", False, [4, 5, 6]),
            ],
            platform="python",
            environment="prod",
            release="foo@1.0.0",
            dist="1",
            transaction_op="http.server",
            transaction_status="ok",
            http_method="GET",
            browser_name="Chrome",
            device_class=2,
            retention_days=30,
        )

        base = {
            "profile_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "project_id": 22,
            "transaction_name": "vroom-vroom",
            "timestamp": now.replace(tzinfo=None),
            "platform": "python",
            "environment": "prod",
            "release": "foo@1.0.0",
            "dist": "1",
            "transaction_op": "http.server",
            "transaction_status": SPAN_STATUS_NAME_TO_CODE["ok"],
            "http_method": "GET",
            "browser_name": "Chrome",
            "device_classification": 2,
            "os_name": "",
            "os_version": "",
            "retention_days": 30,
            "materialization_version": 0,
        }

        batch = [
            {
                "depth": 0,
                "parent_fingerprint": 0,
                "fingerprint": 123,
                "name": "foo",
                "function": "foo",
                "module": "",
                "package": "bar",
                "path": "",
                "is_application": 1,
                "durations": [1, 2, 3],
                **base,
            },
            {
                "depth": 0,
                "parent_fingerprint": 0,
                "fingerprint": 456,
                "name": "baz",
                "function": "baz",
                "module": "qux",
                "package": "",
                "path": "",
                "is_application": 0,
                "durations": [4, 5, 6],
                **base,
            },
        ]

        assert FunctionsMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch(batch, datetime.utcfromtimestamp(message.received))
