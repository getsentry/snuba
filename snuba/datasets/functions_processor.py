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

        for thread, root_frames in message["call_trees"].items():
            for root_frame in root_frames:
                stack = [(root_frame, 0, 0)]
                while stack:
                    frame, depth, parent_fingerprint = stack.pop()
                    if frame["id"] not in functions:
                        functions[frame["id"]] = {
                            "project_id": message["project_id"],
                            "transaction_name": message["transaction_name"],
                            "timestamp": datetime.utcfromtimestamp(
                                message["timestamp"]
                            ),
                            "depth": depth,
                            "parent_fingerprint": parent_fingerprint,
                            "fingerprint": frame["id"],
                            "symbol": frame["name"],
                            "image": frame["package"],
                            "filename": frame.get("path", ""),
                            "is_application": 1
                            if frame.get("is_application", True)
                            else 0,
                            "platform": message["platform"],
                            "environment": message.get("environment"),
                            "release": message.get("release"),
                            "os_name": message["os_name"],
                            "os_version": message["os_version"],
                            "retention_days": message["retention_days"],
                            "durations": [frame["duration_ns"]],
                            "profile_id": str(uuid.UUID(message["profile_id"])),
                            "materialization_version": 0,
                        }
                    else:
                        functions[frame["id"]]["durations"].append(frame["duration_ns"])

                    stack.extend(
                        [
                            (child, depth + 1, frame["id"])
                            for child in frame.get("children", [])
                        ]
                    )

        return InsertBatch(list(functions.values()), None)
