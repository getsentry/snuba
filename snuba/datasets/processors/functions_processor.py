import uuid
from datetime import datetime
from typing import Any, Mapping, Optional

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "functions.processor")

UNKNOWN_SPAN_STATUS = 2
MAX_DEPTH = 1024


class FunctionsMessageProcessor(DatasetMessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        profile_id = str(uuid.UUID(message["profile_id"]))
        status = message.get("transaction_status")
        if status:
            int_status = SPAN_STATUS_NAME_TO_CODE.get(
                status,
                UNKNOWN_SPAN_STATUS,
            )
        else:
            int_status = UNKNOWN_SPAN_STATUS
        functions_list = [
            {
                "project_id": message["project_id"],
                "transaction_name": message["transaction_name"],
                "timestamp": int(
                    message.get("timestamp", datetime.utcnow().timestamp())
                ),
                "depth": 0,  # deprecated
                "parent_fingerprint": 0,  # deprecated
                "fingerprint": function["fingerprint"],
                "name": function["function"],  # to be removed
                "function": function["function"],
                "package": function.get("package", ""),
                "module": function.get("module", ""),
                "path": "",  # deprecated
                "is_application": 1 if function.get("in_app", True) else 0,
                "platform": message["platform"],
                "environment": message.get("environment"),
                "release": message.get("release"),
                "dist": message.get("dist"),
                "transaction_op": message.get("transaction_op", ""),
                "transaction_status": int_status,
                "http_method": message.get("http_method"),
                "browser_name": message.get("browser_name"),
                "device_classification": message.get("device_class", 0),
                "os_name": "",  # deprecated
                "os_version": "",  # deprecated
                "retention_days": message["retention_days"],
                "durations": function["self_times_ns"],
                "profile_id": profile_id,
                "materialization_version": 0,
            }
            for function in message["functions"]
            if function.get("function") and function.get("self_times_ns")
        ]

        received = message.get("received")

        return InsertBatch(
            functions_list,
            datetime.utcfromtimestamp(received) if received else None,
        )
