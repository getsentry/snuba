import json
from datetime import datetime
from typing import cast

import rapidjson
from arroyo.backends.kafka import KafkaPayload

from snuba.datasets.entities import EntityKey
from snuba.query.exceptions import InvalidQueryException
from snuba.subscriptions.data import (
    ScheduledSubscriptionTask,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
    SubscriptionWithTick,
)
from snuba.subscriptions.utils import Tick
from snuba.subscriptions.worker import SubscriptionTaskResult
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

        return SnQLSubscriptionData.from_dict(data, self.entity_key)


class SubscriptionTaskResultEncoder(Encoder[KafkaPayload, SubscriptionTaskResult]):
    def encode(self, value: SubscriptionTaskResult) -> KafkaPayload:
        subscription_id = str(value.task.task.subscription.identifier)
        request, result = value.result
        return KafkaPayload(
            subscription_id.encode("utf-8"),
            json.dumps(
                {
                    "version": 2,
                    "payload": {
                        "subscription_id": subscription_id,
                        "request": {**request.body},
                        "result": result,
                        "timestamp": value.task.timestamp.isoformat(),
                    },
                }
            ).encode("utf-8"),
            [],
        )


class SubscriptionScheduledTaskEncoder(Codec[KafkaPayload, ScheduledSubscriptionTask]):
    """
    Encodes/decodes a scheduled subscription to Kafka payload.
    Does not support non SnQL subscriptions.
    """

    def encode(self, value: ScheduledSubscriptionTask) -> KafkaPayload:
        entity, subscription, tick = value.task

        assert isinstance(subscription.data, SnQLSubscriptionData)

        return KafkaPayload(
            str(subscription.identifier).encode("utf-8"),
            cast(
                str,
                rapidjson.dumps(
                    {
                        "timestamp": value.timestamp.isoformat(),
                        "entity": entity.value,
                        "task": {"data": value.task.subscription.data.to_dict()},
                        "tick": tick.to_dict(),
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
        assert scheduled_subscription_dict["task"]["data"]["type"] == "snql"

        entity_key = EntityKey(scheduled_subscription_dict["entity"])

        return ScheduledSubscriptionTask(
            datetime.fromisoformat(scheduled_subscription_dict["timestamp"]),
            SubscriptionWithTick(
                entity_key,
                Subscription(
                    SubscriptionIdentifier.from_string(subscription_identifier),
                    SnQLSubscriptionData.from_dict(
                        scheduled_subscription_dict["task"]["data"], entity_key
                    ),
                ),
                Tick.from_dict(scheduled_subscription_dict["tick"]),
            ),
        )
