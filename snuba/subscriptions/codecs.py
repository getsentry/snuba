from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Mapping, Union

from snuba.subscriptions.data import (
    PartitionId,
    SubscriptionData,
    SubscriptionIdentifier,
)
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


@dataclass(frozen=True)
class QueryResult:
    subscription_id: SubscriptionIdentifier
    timestamp: datetime
    values: Mapping[str, Union[int, float]]
    partition: PartitionId
    offset: int
    interval: timedelta


class QueryResultCodec(Codec[KafkaPayload, QueryResult]):
    def encode(self, value: QueryResult) -> KafkaPayload:
        subscription_id = str(value.subscription_id)
        return KafkaPayload(
            str(subscription_id).encode("utf-8"),
            json.dumps(
                {
                    "version": 1,
                    "payload": {
                        "subscription_id": str(subscription_id),
                        "values": value.values,
                        "timestamp": value.timestamp.timestamp(),
                        "interval": int(value.interval.total_seconds()),
                        "partition": value.partition,
                        "offset": value.offset,
                    },
                }
            ).encode("utf-8"),
        )

    def decode(self, value: KafkaPayload) -> QueryResult:
        data = json.loads(value.value.decode("utf-8"))
        payload = data["payload"]
        return QueryResult(
            subscription_id=SubscriptionIdentifier.from_string(
                payload["subscription_id"]
            ),
            values=payload["values"],
            timestamp=datetime.fromtimestamp(payload["timestamp"]),
            interval=timedelta(seconds=payload["interval"]),
            partition=PartitionId(payload["partition"]),
            offset=payload["offset"],
        )
