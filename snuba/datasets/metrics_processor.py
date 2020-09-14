from typing import Optional

from snuba.processor import InsertBatch, MessageProcessor, ProcessedMessage
from snuba.datasets.events_format import extract_extra_tags


class MetricsProcessor(MessageProcessor):
    def process_message(self, message, metadata) -> Optional[ProcessedMessage]:
        ret = []

        timestamp = message["timestamp"]
        for k, m in message["metrics"].items():
            keys, values = extract_extra_tags(m["tags"])
            output = {
                "timestamp": timestamp,
                "name": m["name"],
                "tags.key": keys,
                "tags.value": values,
                "count": int(m["count"]),
                "quantiles_sketch": m["quantiles"],
            }
            ret.append(output)

        return InsertBatch(ret)
