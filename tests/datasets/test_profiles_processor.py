import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.profiles_processor import ProfilesMessageProcessor
from snuba.processor import InsertBatch


@dataclass
class ProfileEvent:
    organization_id: int
    project_id: int
    transaction_id: str
    received: int
    profile: str
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
        result["received"] = datetime.utcfromtimestamp(self.received)
        result["offset"] = meta.offset
        result["partition"] = meta.partition
        return result


class TestProfilesProcessor:
    def test_missing_symbols(self) -> None:
        meta = KafkaMessageMetadata(
            offset=1, partition=0, timestamp=datetime(1970, 1, 1)
        )
        message = ProfileEvent(
            organization_id=123456789,
            project_id=987654321,
            transaction_id=str(uuid.uuid4()),
            received=datetime.utcnow().timestamp(),
            profile="someprofile",
            profile_id=str(uuid.uuid4()),
            android_api_level=None,
            device_classification="high",
            device_locale="fr_FR",
            device_manufacturer="Pierre",
            device_model="ThePierrePhone",
            device_os_build_number="13",
            device_os_name="PierreOS",
            device_os_version="47",
            architecture="aarch64",
            duration_ns=1234567890,
            environment="production",
            platform="pierre",
            trace_id=str(uuid.uuid4()),
            transaction_name="lets-get-ready-to-party",
            version_name="v42.0.0",
            version_code="1337",
            retention_days=30,
            partition=meta.partition,
            offset=meta.offset,
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
            organization_id=123456789,
            project_id=987654321,
            transaction_id=str(uuid.uuid4()),
            received=datetime.utcnow().timestamp(),
            profile="someprofile",
            profile_id=str(uuid.uuid4()),
            android_api_level=None,
            device_classification="high",
            device_locale="fr_FR",
            device_manufacturer="Pierre",
            device_model="ThePierrePhone",
            device_os_build_number="13",
            device_os_name="PierreOS",
            device_os_version="47",
            architecture="aarch64",
            duration_ns=1234567890,
            environment="production",
            platform="pierre",
            trace_id=str(uuid.uuid4()),
            transaction_name="lets-get-ready-to-party",
            version_name="v42.0.0",
            version_code="1337",
            retention_days=30,
            partition=meta.partition,
            offset=meta.offset,
        )
        assert ProfilesMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch([message.build_result(meta)], None)
