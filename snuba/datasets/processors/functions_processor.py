import uuid
from datetime import datetime
from typing import Any, Mapping, Optional

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "functions.processor")

MAX_DEPTH = 1024


class FunctionsMessageProcessor(DatasetMessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        max_depth_reached = False

        functions = {}

        profile_id = str(uuid.UUID(message["profile_id"]))
        timestamp = datetime.utcfromtimestamp(message["timestamp"])

        for thread, root_frames in message["call_trees"].items():
            for root_frame in root_frames:
                stack = [(root_frame, 0, 0)]
                while stack:
                    frame, depth, parent_fingerprint = stack.pop()
                    if frame["fingerprint"] not in functions:
                        functions[frame["fingerprint"]] = {
                            "project_id": message["project_id"],
                            "transaction_name": message["transaction_name"],
                            "timestamp": timestamp,
                            "depth": depth,
                            "parent_fingerprint": parent_fingerprint,
                            "fingerprint": frame["fingerprint"],
                            "name": frame["name"],
                            "package": frame["package"],
                            "path": frame.get("path", ""),
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
                            "profile_id": profile_id,
                            "materialization_version": 0,
                        }
                    else:
                        functions[frame["fingerprint"]]["durations"].append(
                            frame["duration_ns"]
                        )

                    children = frame.get("children", [])

                    if depth < MAX_DEPTH:
                        stack.extend(
                            [
                                (child, depth + 1, frame["fingerprint"])
                                for child in children
                            ]
                        )
                    elif children:
                        max_depth_reached = True

        if max_depth_reached:
            metrics.increment("max_depth_reached")

        return InsertBatch(list(functions.values()), None)
