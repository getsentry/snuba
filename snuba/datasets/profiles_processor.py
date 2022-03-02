import logging
from typing import Any, Mapping, Optional
from uuid import UUID

from dateutil.parser import parse

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch, MessageProcessor, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)
metrics = MetricsWrapper(environment.metrics, "profiles.processor")

RETENTION_DAYS_ALLOWED = frozenset([30, 90])


class ProfilesMessageProcessor(MessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        try:
            retention_days = message["retention_days"]
            if retention_days not in RETENTION_DAYS_ALLOWED:
                retention_days = 30
            processed = {
                "organization_id": message["organization_id"],
                "project_id": message["project_id"],
                "transaction_id": str(UUID(message["transaction_id"])),
                "received": parse(message["received"]),
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
                "platform": message["platform"],
                "trace_id": str(UUID(message["trace_id"])),
                "transaction_name": message["transaction_name"],
                "version_name": message["version_name"],
                "version_code": message["version_code"],
                "retention_days": retention_days,
                "offset": metadata.offset,
                "partition": metadata.partition,
            }
        except ValueError:
            logger.warning(
                "Invalid UUID",
                extra={
                    "transaction_id": message["transaction_id"],
                    "trace_id": message["trace_id"],
                },
            )
        except KeyError:
            metrics.increment("missing_field")
            return None
        return InsertBatch([processed], None)
