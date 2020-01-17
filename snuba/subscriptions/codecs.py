import json
from datetime import timedelta

from snuba.subscriptions.data import Subscription
from snuba.utils.codecs import Codec


class SubscriptionCodec(Codec[bytes, Subscription]):
    def encode(self, value: Subscription) -> bytes:
        return json.dumps(
            {
                "project_id": value.project_id,
                "conditions": value.conditions,
                "aggregations": value.aggregations,
                "time_window": int(value.time_window.total_seconds()),
                "resolution": int(value.resolution.total_seconds()),
            }
        ).encode("utf-8")

    def decode(self, value: bytes) -> Subscription:
        data = json.loads(value.decode("utf-8"))
        return Subscription(
            project_id=data["project_id"],
            conditions=data["conditions"],
            aggregations=data["aggregations"],
            time_window=timedelta(seconds=data["time_window"]),
            resolution=timedelta(seconds=data["resolution"]),
        )
