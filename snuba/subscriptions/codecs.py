from __future__ import annotations

import json
from concurrent.futures import Future
from datetime import timedelta
from typing import Any, NamedTuple

from snuba.subscriptions.data import Subscription, SubscriptionData
from snuba.subscriptions.task import ScheduledTask
from snuba.utils.codecs import Codec
from snuba.utils.streams.kafka import KafkaPayload


class SubscriptionDataCodec(Codec[bytes, SubscriptionData]):
    def encode(self, value: SubscriptionData) -> bytes:
        return json.dumps(
            {
                "project_id": value.project_id,
                "conditions": value.conditions,
                "aggregations": value.aggregations,
                "time_window": int(value.time_window.total_seconds()),
                "resolution": int(value.resolution.total_seconds()),
            }
        ).encode("utf-8")

    def decode(self, value: bytes) -> SubscriptionData:
        data = json.loads(value.decode("utf-8"))
        return SubscriptionData(
            project_id=data["project_id"],
            conditions=data["conditions"],
            aggregations=data["aggregations"],
            time_window=timedelta(seconds=data["time_window"]),
            resolution=timedelta(seconds=data["resolution"]),
        )


# XXX: Temporary types until another pr merges
Result = Any


class SubscriptionResult(NamedTuple):
    task: ScheduledTask[Subscription]
    future: Future[Result]


class SubscriptionResultCodec(Codec[KafkaPayload, SubscriptionResult]):
    def encode(self, value: SubscriptionResult) -> KafkaPayload:
        query_result = value.future.result(timeout=5.0)
        return KafkaPayload(
            # TODO: Not sure if this is right, but we likely want to rely on the same
            # partition logic as elsewhere, and I believe we partition on project_id as
            # key
            str(value.task.task.data.project_id).encode("utf-8"),
            json.dumps(
                {
                    "version": 1,
                    "payload": {
                        "subscription_id": str(value.task.task.identifier),
                        "values": {
                            key: val
                            for key, val in query_result["data"].items()
                            if key not in ("max_offset", "max_partition")
                        },
                        "timestamp": value.task.timestamp,
                        # Are these actually useful?
                        "interval": int(
                            value.task.task.data.time_window.total_seconds()
                        ),
                        "partition": query_result["data"]["max_partition"],
                        "offset": query_result["data"]["max_offset"],
                    },
                }
            ).encode("utf-8"),
        )

    def decode(self, value: KafkaPayload) -> SubscriptionResult:
        # Not really possible to decode this back into a `SubscriptionResult`, should
        # we have a different type of codec that expects this?
        return json.loads(value.value.decode("utf-8"))
