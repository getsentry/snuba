from datetime import datetime
from typing import Any, Mapping, Optional
from uuid import UUID

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch, MessageProcessor, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "profiles.processor")


class ProfilesMessageProcessor(MessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        try:
            if "version" in message:
                additional_fields = {
                    key: message["device"].get(key, "")
                    for key in [
                        "locale",
                        "classification",
                        "manufacturer",
                        "model",
                        "architecture",
                    ]
                }
                additional_fields["version_name"] = message["release"]

                for field in ["name", "version"]:
                    additional_fields[field] = message["os"][field]
            else:
                additional_fields = {
                    key: message.get(f"device_{key}", "")
                    for key in ["locale", "classification", "manufacturer", "model"]
                }
                additional_fields["architecture"] = message.get(
                    "architecture", "unknown"
                )
                additional_fields["version_name"] = message["version_name"]
                additional_fields["device_os_name"] = message["device_os_name"]
                additional_fields["device_os_version"] = message["device_os_version"]

            processed = {
                "android_api_level": message.get("android_api_level"),
                "device_os_build_number": message.get("device_os_build_number"),
                "duration_ns": message["duration_ns"],
                "environment": message.get("environment"),
                "offset": metadata.offset,
                "organization_id": message["organization_id"],
                "partition": metadata.partition,
                "platform": message["platform"],
                "profile": "",  # deprecated
                "profile_id": str(UUID(message["profile_id"])),
                "project_id": message["project_id"],
                "received": datetime.utcfromtimestamp(message["received"]),
                "retention_days": message["retention_days"],
                "trace_id": str(UUID(message["trace_id"])),
                "transaction_id": str(UUID(message["transaction_id"])),
                "transaction_name": message["transaction_name"],
                "version_code": message.get("version_code", ""),
            }
            processed.update(additional_fields)
        except ValueError:
            metrics.increment("invalid_uuid")
            return None
        except KeyError:
            metrics.increment("missing_field")
            return None
        return InsertBatch([processed], None)
