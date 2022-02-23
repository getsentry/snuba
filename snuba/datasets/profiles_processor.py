from typing import Any, Mapping, Optional

from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch, MessageProcessor, ProcessedMessage


class ProfilesProcessor(MessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        processed = {
            "organization_id": message["organization_id"],
            "project_id": message["project_id"],
            "transaction_id": message["transaction_id"],
            "received": message["received"],
            "profile": message["profile"],
            "symbols": message["symbols"],
            "android_api_level": message.get("android_api_level"),
            "device_classification": message["device_classification"],
            "device_locale": message["device_locale"],
            "device_manufacturer": message["device_manufacturer"],
            "device_model": message["device_model"],
            "device_os_build_number": message.get("device_os_build_number"),
            "device_os_name": message["device_os_name"],
            "device_os_version": message["device_os_version"],
            "duration_ns": message["duration_ns"],
            "environment": message.get("environment"),
            "error_code": message.get("error_code"),
            "error_description": message.get("error_description"),
            "platform": message["platform"],
            "trace_id": message["trace_id"],
            "version": message["version"],
            "retention_days": 30,
        }
        return InsertBatch([processed], None)
