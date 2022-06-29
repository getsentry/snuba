import uuid
from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch, MessageProcessor, ProcessedMessage


class FunctionsMessageProcessor(MessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        functions = {}

        for root_frame in message["call_trees"]:
            stack = [root_frame]
            while stack:
                frame = stack.pop()
                if frame["fingerprint"] not in functions:
                    functions[frame["fingerprint"]] = {
                        "project_id": message["project_id"],
                        "transaction_name": message["transaction_name"],
                        "timestamp": datetime.utcfromtimestamp(message["timestamp"]),
                        "depth": frame["depth"],
                        "parent_fingerprint": int(frame["parent_fingerprint"], 16),
                        "fingerprint": int(frame["fingerprint"], 16),
                        "symbol": frame["symbol"],
                        "image": frame["image"],
                        "filename": frame["filename"],
                        "is_application": 1 if frame["is_application"] else 0,
                        "platform": message["platform"],
                        "environment": message.get("environment"),
                        "release": message["release"],
                        "os_name": message["os_name"],
                        "os_version": message["os_version"],
                        "retention_days": message["retention_days"],
                        "durations": [frame["duration"]],
                        "profile_id": str(uuid.UUID(message["profile_id"])),
                        "materialization_version": 0,
                    }
                else:
                    functions[frame["fingerprint"]]["durations"].append(
                        frame["duration"]
                    )

                stack.extend(frame["children"])

        return InsertBatch(list(functions.values()), None)
