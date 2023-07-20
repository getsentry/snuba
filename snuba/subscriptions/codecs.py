import json
from datetime import datetime
from typing import cast

import rapidjson
from arroyo.backends.kafka import KafkaPayload
from sentry_kafka_schemas.schema_types import events_subscription_results_v1

from snuba.datasets.entities.entity_key import EntityKey
from snuba.query.exceptions import InvalidQueryException
from snuba.subscriptions.data import (
    ScheduledSubscriptionTask,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
    SubscriptionTaskResult,
    SubscriptionWithMetadata,
)
from snuba.utils.codecs import Codec, Encoder


class SubscriptionDataCodec(Codec[bytes, SubscriptionData]):
    def __init__(self, entity_key: EntityKey):
        self.entity_key = entity_key

    def encode(self, value: SubscriptionData) -> bytes:
        return json.dumps(value.to_dict()).encode("utf-8")

    def decode(self, value: bytes) -> SubscriptionData:
        try:
            data = json.loads(value.decode("utf-8"))
        except json.JSONDecodeError:
            raise InvalidQueryException("Invalid JSON")

        return SubscriptionData.from_dict(data, self.entity_key)


class SubscriptionTaskResultEncoder(Encoder[KafkaPayload, SubscriptionTaskResult]):
    def encode(self, value: SubscriptionTaskResult) -> KafkaPayload:
        entity, subscription, _ = value.task.task
        subscription_id = str(subscription.identifier)
        request, result = value.result

        data: events_subscription_results_v1.SubscriptionResult = {
            "version": 3,
            "payload": {
                "subscription_id": subscription_id,
                "request": {**request.original_body},
                "result": {
                    "data": result["data"],
                    "meta": result["meta"],
                },
                "timestamp": value.task.timestamp.isoformat(),
                "entity": entity.value,
            },
        }

        return KafkaPayload(
            subscription_id.encode("utf-8"),
            json.dumps(data).encode("utf-8"),
            [],
        )


class SubscriptionScheduledTaskEncoder(Codec[KafkaPayload, ScheduledSubscriptionTask]):
    """
    Encodes/decodes a scheduled subscription to Kafka payload.
    Does not support non SnQL subscriptions.
    """

    def encode(self, value: ScheduledSubscriptionTask) -> KafkaPayload:
        entity, subscription, tick_upper_offset = value.task

        return KafkaPayload(
            str(subscription.identifier).encode("utf-8"),
            cast(
                str,
                rapidjson.dumps(
                    {
                        "timestamp": value.timestamp.isoformat(),
                        "entity": entity.value,
                        "task": {"data": subscription.data.to_dict()},
                        "tick_upper_offset": tick_upper_offset,
                    }
                ),
            ).encode("utf-8"),
            [],
        )

    def decode(self, value: KafkaPayload) -> ScheduledSubscriptionTask:
        payload_value = value.value

        assert value.key is not None
        subscription_identifier = value.key.decode("utf-8")

        scheduled_subscription_dict = rapidjson.loads(payload_value.decode("utf-8"))

        entity_key = EntityKey(scheduled_subscription_dict["entity"])

        return ScheduledSubscriptionTask(
            datetime.fromisoformat(scheduled_subscription_dict["timestamp"]),
            SubscriptionWithMetadata(
                entity_key,
                Subscription(
                    SubscriptionIdentifier.from_string(subscription_identifier),
                    SubscriptionData.from_dict(
                        scheduled_subscription_dict["task"]["data"], entity_key
                    ),
                ),
                scheduled_subscription_dict["tick_upper_offset"],
            ),
        )
