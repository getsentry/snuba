import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.profiles_processor import ProfilesMessageProcessor
from snuba.processor import InsertBatch


@dataclass
class SampleProfileEvent:
    device: dict
    environment: Optional[str]
    offset: int
    organization_id: int
    os: dict
    partition: int
    platform: str
    event_id: str
    project_id: int
    received: int
    release: str
    retention_days: int
    timestamp: str
    transactions: list
    version: str

    def serialize(self) -> Mapping[str, Any]:
        return asdict(self)

    def build_result(self, meta: KafkaMessageMetadata) -> Mapping[str, Any]:
        transaction = self.transactions[0]
        return {
            "android_api_level": None,
            "offset": meta.offset,
            "partition": meta.partition,
            "received": int(self.received),
            "architecture": "arm64e",
            "device_classification": "high",
            "device_locale": "fr_FR",
            "device_manufacturer": "Pierre",
            "device_model": "ThePierrePhone",
            "device_os_build_number": "13",
            "device_os_name": "PierreOS",
            "device_os_version": "47",
            "duration_ns": 1234567890,
            "environment": "production",
            "organization_id": 123456789,
            "platform": "cocoa",
            "profile_id": self.event_id,
            "project_id": 987654321,
            "retention_days": 30,
            "trace_id": transaction["trace_id"],
            "transaction_id": transaction["id"],
            "transaction_name": transaction["name"],
            "version_code": "",
            "version_name": "v42.0.0 (999)",
        }


@dataclass
class ProfileEvent:
    organization_id: int
    project_id: int
    transaction_id: str
    received: int
    profile_id: str
    android_api_level: Optional[int]
    device_classification: str
    device_locale: str
    device_manufacturer: str
    device_model: str
    device_os_build_number: str
    device_os_name: str
    device_os_version: str
    architecture: str
    duration_ns: int
    environment: Optional[str]
    platform: str
    trace_id: str
    transaction_name: str
    version_name: str
    version_code: str
    retention_days: int
    offset: int
    partition: int

    def serialize(self) -> Mapping[str, Any]:
        return asdict(self)

    def build_result(self, meta: KafkaMessageMetadata) -> Mapping[str, Any]:
        result = asdict(self)
        result["received"] = int(self.received)
        result["offset"] = meta.offset
        result["partition"] = meta.partition
        return result


class TestProfilesProcessor:
    def test_sample_profile_message(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )
        message = SampleProfileEvent(
            organization_id=123456789,
            project_id=987654321,
            transactions=[
                {
                    "id": str(uuid.uuid4()),
                    "name": "lets-get-ready-to-party",
                    "trace_id": str(uuid.uuid4()),
                    "relative_start_ns": 0,
                    "relative_end_ns": 1234567890,
                }
            ],
            device={
                "classification": "high",
                "locale": "fr_FR",
                "manufacturer": "Pierre",
                "model": "ThePierrePhone",
                "architecture": "arm64e",
            },
            os={
                "build_number": "13",
                "name": "PierreOS",
                "version": "47",
            },
            timestamp=datetime.utcnow().isoformat(),
            received=int(datetime.utcnow().timestamp()),
            event_id=str(uuid.uuid4()),
            environment="production",
            platform="cocoa",
            release="v42.0.0 (999)",
            version="1",
            retention_days=30,
            partition=meta.partition,
            offset=meta.offset,
        )
        assert ProfilesMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch(
            [message.build_result(meta)], datetime.utcfromtimestamp(message.received)
        )

    def test_missing_symbols(self) -> None:
        meta = KafkaMessageMetadata(
            offset=1, partition=0, timestamp=datetime(1970, 1, 1)
        )
        message = ProfileEvent(
            android_api_level=None,
            architecture="aarch64",
            device_classification="high",
            device_locale="fr_FR",
            device_manufacturer="Pierre",
            device_model="ThePierrePhone",
            device_os_build_number="13",
            device_os_name="PierreOS",
            device_os_version="47",
            duration_ns=1234567890,
            environment="production",
            offset=meta.offset,
            organization_id=123456789,
            partition=meta.partition,
            platform="pierre",
            profile_id=str(uuid.uuid4()),
            project_id=987654321,
            received=int(datetime.utcnow().timestamp()),
            retention_days=30,
            trace_id=str(uuid.uuid4()),
            transaction_id=str(uuid.uuid4()),
            transaction_name="lets-get-ready-to-party",
            version_code="1337",
            version_name="v42.0.0",
        )
        payload = message.serialize()
        del payload["profile_id"]
        processor = ProfilesMessageProcessor()
        assert processor.process_message(payload, meta) is None

    def test_process_message(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )
        message = ProfileEvent(
            android_api_level=None,
            architecture="aarch64",
            device_classification="high",
            device_locale="fr_FR",
            device_manufacturer="Pierre",
            device_model="ThePierrePhone",
            device_os_build_number="13",
            device_os_name="PierreOS",
            device_os_version="47",
            duration_ns=1234567890,
            environment="production",
            offset=meta.offset,
            organization_id=123456789,
            partition=meta.partition,
            platform="pierre",
            profile_id=str(uuid.uuid4()),
            project_id=987654321,
            received=int(datetime.utcnow().timestamp()),
            retention_days=30,
            trace_id=str(uuid.uuid4()),
            transaction_id=str(uuid.uuid4()),
            transaction_name="lets-get-ready-to-party",
            version_code="1337",
            version_name="v42.0.0",
        )
        assert ProfilesMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch(
            [message.build_result(meta)], datetime.utcfromtimestamp(message.received)
        )
