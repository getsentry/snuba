import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Mapping, Optional

from dateutil.parser import parse

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.profiles_processor import ProfilesMessageProcessor
from snuba.processor import InsertBatch


@dataclass
class ProfileEvent:
    organization_id: int
    project_id: int
    transaction_id: str
    received: str
    profile: str
    symbols: Optional[str]
    android_api_level: Optional[int]
    device_classification: str
    device_locale: str
    device_manufacturer: str
    device_model: str
    device_os_build_number: str
    device_os_name: str
    device_os_version: str
    duration_ns: int
    environment: Optional[str]
    platform: str
    trace_id: str
    transaction_name: str
    version_name: str
    version_code: str

    def serialize(self) -> Mapping[str, Any]:
        return asdict(self)

    def build_result(self, meta: KafkaMessageMetadata) -> Mapping[str, Any]:
        result = asdict(self)
        result["received"] = parse(self.received)
        result["retention_days"] = 30
        return result


class TestProfilesProcessor:
    def test_missing_symbols(self) -> None:
        message = ProfileEvent(
            organization_id=123456789,
            project_id=987654321,
            transaction_id=str(uuid.uuid4()),
            received=datetime.utcnow().isoformat(),
            profile="someprofile",
            symbols=None,
            android_api_level=None,
            device_classification="high",
            device_locale="fr_FR",
            device_manufacturer="Pierre",
            device_model="ThePierrePhone",
            device_os_build_number="13",
            device_os_name="PierreOS",
            device_os_version="47",
            duration_ns=1234567890,
            environment="production",
            platform="pierre",
            trace_id=str(uuid.uuid4()),
            transaction_name="lets-get-ready-to-party",
            version_name="v42.0.0",
            version_code="1337",
        )
        payload = message.serialize()
        del payload["symbols"]
        meta = KafkaMessageMetadata(
            offset=1, partition=0, timestamp=datetime(1970, 1, 1)
        )
        processor = ProfilesMessageProcessor()
        assert processor.process_message(payload, meta) is None

    def test_process_message(self) -> None:
        message = ProfileEvent(
            organization_id=123456789,
            project_id=987654321,
            transaction_id=str(uuid.uuid4()),
            received=datetime.utcnow().isoformat(),
            profile="someprofile",
            symbols="somesymbols",
            android_api_level=None,
            device_classification="high",
            device_locale="fr_FR",
            device_manufacturer="Pierre",
            device_model="ThePierrePhone",
            device_os_build_number="13",
            device_os_name="PierreOS",
            device_os_version="47",
            duration_ns=1234567890,
            environment="production",
            platform="pierre",
            trace_id=str(uuid.uuid4()),
            transaction_name="lets-get-ready-to-party",
            version_name="v42.0.0",
            version_code="1337",
        )
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )
        assert ProfilesMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch([message.build_result(meta)], None)
