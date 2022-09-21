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
                    f"device_{field}": message["device"].get(field, "")
                    for field in [
                        "locale",
                        "classification",
                        "manufacturer",
                        "model",
                    ]
                }
                additional_fields["architecture"] = message["device"].get(
                    "architecture", "unknown"
                )
                additional_fields["version_name"] = message["release"]

                for field in ["name", "version", "build_number"]:
                    additional_fields[f"device_os_{field}"] = message["os"].get(field)

                transaction = message["transactions"][0]
                additional_fields.update(
                    {
                        "transaction_id": transaction["id"],
                        "transaction_name": transaction["name"],
                        "trace_id": transaction["trace_id"],
                        "duration_ns": int(
                            transaction["relative_end_ns"]
                            - transaction["relative_start_ns"]
                        ),
                        "profile_id": str(UUID(message["event_id"])),
                    }
                )
            else:
                additional_fields = {
                    f"device_{field}": message.get(f"device_{field}", "")
                    for field in ["locale", "classification", "manufacturer", "model"]
                }
                additional_fields["architecture"] = message.get(
                    "architecture", "unknown"
                )
                additional_fields["version_name"] = message["version_name"]
                additional_fields["device_os_name"] = message["device_os_name"]
                additional_fields["device_os_version"] = message["device_os_version"]
                additional_fields["duration_ns"] = message["duration_ns"]
                additional_fields["transaction_name"] = message["transaction_name"]
                additional_fields["transaction_id"] = str(
                    UUID(message["transaction_id"])
                )
                additional_fields["trace_id"] = str(UUID(message["trace_id"]))
                additional_fields["profile_id"] = str(UUID(message["profile_id"]))

            processed = {
                "android_api_level": message.get("android_api_level"),
                "device_os_build_number": message.get("device_os_build_number"),
                "environment": message.get("environment"),
                "offset": metadata.offset,
                "organization_id": message["organization_id"],
                "partition": metadata.partition,
                "platform": message["platform"],
                "profile": "",  # deprecated
                "project_id": message["project_id"],
                "received": datetime.utcfromtimestamp(message["received"]),
                "retention_days": message["retention_days"],
                "version_code": message.get("version_code", ""),
            }
            processed.update(additional_fields)
        except IndexError:
            metrics.increment("invalid_transaction")
            return None
        except ValueError:
            metrics.increment("invalid_uuid")
            return None
        except KeyError:
            metrics.increment("missing_field")
            return None
        return InsertBatch([processed], None)
