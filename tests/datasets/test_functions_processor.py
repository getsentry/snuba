from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.functions_processor import FunctionsMessageProcessor
from snuba.processor import InsertBatch


@dataclass
class Function:
    fingerprint: int
    function: str
    package: str
    in_app: bool
    self_times_ns: Sequence[int]

    def serialize(self) -> Mapping[str, Any]:
        return {
            "fingerprint": self.fingerprint,
            "function": self.function,
            "package": self.package,
            "in_app": self.in_app,
            "self_times_ns": self.self_times_ns,
        }


@dataclass
class ProfileFunctionsEvent:
    project_id: int
    profile_id: str
    transaction_name: str
    timestamp: int
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
    received: Optional[int]

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
    def test_process_message_functions(self) -> None:
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )

        now = int(datetime.now(timezone.utc).timestamp())
        message = ProfileFunctionsEvent(
            project_id=22,
            profile_id="a" * 32,
            transaction_name="vroom-vroom",
            timestamp=now,
            received=now,
            functions=[
                Function(123, "foo", "bar", True, [1, 2, 3]),
                Function(456, "baz", "", False, [4, 5, 6]),
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
            "timestamp": now,
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
                "package": "",
                "path": "",
                "is_application": 0,
                "durations": [4, 5, 6],
                **base,
            },
        ]

        assert FunctionsMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch(
            rows=batch, origin_timestamp=None, sentry_received_timestamp=None
        )
