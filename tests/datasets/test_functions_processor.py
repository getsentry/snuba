from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.functions_processor import FunctionsMessageProcessor
from snuba.processor import InsertBatch


@dataclass
class CallTree:
    depth: int
    parent_fingerprint: str
    fingerprint: str
    symbol: str
    image: str
    filename: str
    is_application: bool
    duration: float
    children: Sequence["CallTree"]

    def serialize(self) -> Mapping[str, Any]:
        return {
            "depth": self.depth,
            "parent_fingerprint": self.parent_fingerprint,
            "fingerprint": self.fingerprint,
            "symbol": self.symbol,
            "image": self.image,
            "filename": self.filename,
            "is_application": self.is_application,
            "duration": self.duration,
            "children": [ct.serialize() for ct in self.children],
        }


@dataclass
class ProfileCallTreeEvent:
    project_id: int
    profile_id: str
    transaction_name: str
    timestamp: int
    call_trees: Sequence[CallTree]
    platform: str
    environment: Optional[str]
    release: Optional[str]
    os_name: str
    os_version: str
    retention_days: int

    def serialize(self) -> Mapping[str, Any]:
        return {
            "project_id": self.project_id,
            "profile_id": self.profile_id,
            "transaction_name": self.transaction_name,
            "timestamp": self.timestamp,
            "call_trees": [ct.serialize() for ct in self.call_trees],
            "platform": self.platform,
            "environment": self.environment,
            "release": self.release,
            "os_name": self.os_name,
            "os_version": self.os_version,
            "retention_days": self.retention_days,
        }


class TestFunctionsProcessor:
    def test_process_message(self) -> None:
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )

        message = ProfileCallTreeEvent(
            project_id=22,
            profile_id="a" * 32,
            timestamp=0,
            transaction_name="vroom-vroom",
            call_trees=[
                CallTree(
                    depth=0,
                    parent_fingerprint="0" * 16,
                    fingerprint="a" * 16,
                    symbol="foo",
                    image="",
                    filename="",
                    is_application=True,
                    duration=10,
                    children=[
                        CallTree(
                            depth=1,
                            parent_fingerprint="a" * 16,
                            fingerprint="c" * 16,
                            symbol="bar",
                            image="",
                            filename="",
                            is_application=False,
                            duration=5,
                            children=[],
                        ),
                    ],
                ),
                CallTree(
                    depth=0,
                    parent_fingerprint="0" * 16,
                    fingerprint="b" * 16,
                    symbol="baz",
                    image="",
                    filename="",
                    is_application=True,
                    duration=5,
                    children=[],
                ),
                CallTree(
                    depth=0,
                    parent_fingerprint="0" * 16,
                    fingerprint="a" * 16,
                    symbol="foo",
                    image="",
                    filename="",
                    is_application=True,
                    duration=20,
                    children=[
                        CallTree(
                            depth=1,
                            parent_fingerprint="a" * 16,
                            fingerprint="c" * 16,
                            symbol="bar",
                            image="",
                            filename="",
                            is_application=True,
                            duration=10,
                            children=[],
                        ),
                    ],
                ),
            ],
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
            "timestamp": datetime(1970, 1, 1),
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
                "symbol": "foo",
                "image": "",
                "filename": "",
                "is_application": 1,
                "durations": [10, 20],
                **base,
            },
            {
                "depth": 1,
                "parent_fingerprint": int("a" * 16, 16),
                "fingerprint": int("c" * 16, 16),
                "symbol": "bar",
                "image": "",
                "filename": "",
                "is_application": 0,
                "durations": [5, 10],
                **base,
            },
            {
                "depth": 0,
                "parent_fingerprint": 0,
                "fingerprint": int("b" * 16, 16),
                "symbol": "baz",
                "image": "",
                "filename": "",
                "is_application": 1,
                "durations": [5],
                **base,
            },
        ]

        assert FunctionsMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch(batch, None)
