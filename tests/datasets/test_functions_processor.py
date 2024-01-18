from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

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
    environment: Optional[str]
    functions: Sequence[Function]
    platform: str
    profile_id: str
    project_id: int
    received: Optional[int]
    release: Optional[str]
    retention_days: int
    timestamp: int
    transaction_name: str

    def serialize(self) -> Mapping[str, Any]:
        return {
            "environment": self.environment,
            "functions": [f.serialize() for f in self.functions],
            "platform": self.platform,
            "profile_id": self.profile_id,
            "project_id": self.project_id,
            "received": self.received,
            "release": self.release,
            "retention_days": self.retention_days,
            "timestamp": self.timestamp,
            "transaction_name": self.transaction_name,
        }


class TestFunctionsProcessor:
    def test_process_message_functions(self) -> None:
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )

        now = int(datetime.now(timezone.utc).timestamp())
        message = ProfileFunctionsEvent(
            environment="prod",
            functions=[
                Function(123, "foo", "bar", True, [1, 2, 3]),
                Function(456, "baz", "", False, [4, 5, 6]),
            ],
            platform="python",
            profile_id="a" * 32,
            project_id=22,
            received=now,
            release="foo@1.0.0",
            retention_days=30,
            timestamp=now,
            transaction_name="vroom-vroom",
        )

        base = {
            # function metadata fields
            "environment": "prod",
            "platform": "python",
            "profile_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "project_id": 22,
            "release": "foo@1.0.0",
            "retention_days": 30,
            "timestamp": now,
            "transaction_name": "vroom-vroom",
            # snuba fields
            "materialization_version": 0,
            # deprecated fields
            "browser_name": "",
            "depth": 0,
            "device_classification": 0,
            "dist": "",
            "os_name": "",
            "os_version": "",
            "parent_fingerprint": 0,
            "path": "",
            "transaction_op": "",
            "transaction_status": 0,
        }

        batch = [
            {
                "durations": [1, 2, 3],
                "fingerprint": 123,
                "is_application": 1,
                "name": "foo",
                "package": "bar",
                **base,
            },
            {
                "durations": [4, 5, 6],
                "fingerprint": 456,
                "is_application": 0,
                "name": "baz",
                "package": "",
                **base,
            },
        ]

        assert FunctionsMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch(rows=batch, origin_timestamp=None)
