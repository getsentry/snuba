from datetime import datetime
from typing import Any, Mapping, Optional
from uuid import UUID

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "profiles.processor")


class ProfilesMessageProcessor(DatasetMessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        try:
            received = datetime.utcfromtimestamp(message["received"])
            retention_days = enforce_retention(message["retention_days"], received)

            if "version" in message:
                processed = _normalize_sample_format(
                    message, metadata, retention_days, received
                )
            else:
                processed = _normalize_legacy_format(
                    message, metadata, retention_days, received
                )
        except EventTooOld:
            metrics.increment("event_too_old")
            return None
        except IndexError:
            metrics.increment("invalid_transaction")
            return None
        except ValueError:
            metrics.increment("invalid_uuid")
            return None
        except KeyError:
            metrics.increment("missing_field")
            return None
        return InsertBatch([processed], received)


def _normalize_legacy_format(
    message: Mapping[str, Any],
    metadata: KafkaMessageMetadata,
    retention_days: int,
    received: datetime,
) -> Mapping[str, Any]:
    return {
        "android_api_level": message.get("android_api_level"),
        "architecture": message.get("architecture", "unknown"),
        "device_classification": message.get("device_classification", ""),
        "device_locale": message["device_locale"],
        "device_manufacturer": message["device_manufacturer"],
        "device_model": message["device_model"],
        "device_os_build_number": message.get("device_os_build_number"),
        "device_os_name": message["device_os_name"],
        "device_os_version": message["device_os_version"],
        "duration_ns": message["duration_ns"],
        "environment": message.get("environment"),
        "offset": metadata.offset,
        "organization_id": message["organization_id"],
        "partition": metadata.partition,
        "platform": message["platform"],
        "profile_id": str(UUID(message["profile_id"])),
        "project_id": message["project_id"],
        "received": message["received"],
        "retention_days": retention_days,
        "trace_id": str(UUID(message["trace_id"])),
        "transaction_id": str(UUID(message["transaction_id"])),
        "transaction_name": message["transaction_name"],
        "version_code": message["version_code"],
        "version_name": message["version_name"],
    }


def _normalize_sample_format(
    message: Mapping[str, Any],
    metadata: KafkaMessageMetadata,
    retention_days: int,
    received: datetime,
) -> Mapping[str, Any]:
    transaction = message["transactions"][0]
    device = message["device"]
    os = message["os"]
    return {
        "android_api_level": message.get("android_api_level"),
        "architecture": device.get("architecture", "unknown"),
        "device_classification": device.get("classification", ""),
        "device_locale": device.get("locale", ""),
        "device_manufacturer": device.get("manufacturer", ""),
        "device_model": device.get("model", ""),
        "device_os_build_number": os.get("build_number"),
        "device_os_name": os.get("name", ""),
        "device_os_version": os.get("version", ""),
        "duration_ns": int(
            transaction["relative_end_ns"] - transaction["relative_start_ns"]
        ),
        "environment": message.get("environment"),
        "offset": metadata.offset,
        "organization_id": message["organization_id"],
        "partition": metadata.partition,
        "platform": message["platform"],
        "profile_id": str(UUID(message["event_id"])),
        "project_id": message["project_id"],
        "received": received,
        "retention_days": retention_days,
        "trace_id": str(UUID(transaction["trace_id"])),
        "transaction_id": str(UUID(transaction["id"])),
        "transaction_name": transaction["name"],
        "version_code": message.get("version_code", ""),
        "version_name": message["release"],
    }
