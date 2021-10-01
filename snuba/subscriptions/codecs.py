import json
from datetime import datetime
from typing import cast

import rapidjson
from arroyo.backends.kafka import KafkaPayload

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import ENTITY_NAME_LOOKUP
from snuba.datasets.entity import Entity
from snuba.query.exceptions import InvalidQueryException
from snuba.subscriptions.data import (
    DelegateSubscriptionData,
    LegacySubscriptionData,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
)
from snuba.subscriptions.entity_subscription import (
    InvalidSubscriptionError,
    SubscriptionType,
)
from snuba.subscriptions.worker import SubscriptionTaskResult
from snuba.utils.codecs import Codec, Encoder
from snuba.utils.scheduler import ScheduledTask


class SubscriptionDataCodec(Codec[bytes, SubscriptionData]):
    def __init__(self, entity: Entity):
        self.entity_key = ENTITY_NAME_LOOKUP[entity]

    def encode(self, value: SubscriptionData) -> bytes:
        return json.dumps(value.to_dict()).encode("utf-8")

    def decode(self, value: bytes) -> SubscriptionData:
        try:
            data = json.loads(value.decode("utf-8"))
        except json.JSONDecodeError:
            raise InvalidQueryException("Invalid JSON")

        subscription_type = data.get(SubscriptionData.TYPE_FIELD)
        if subscription_type == SubscriptionType.SNQL.value:
            return SnQLSubscriptionData.from_dict(data, self.entity_key)
        elif subscription_type == SubscriptionType.DELEGATE.value:
            return DelegateSubscriptionData.from_dict(data, self.entity_key)
        elif subscription_type is None:
            return LegacySubscriptionData.from_dict(data, self.entity_key)
        else:
            raise InvalidSubscriptionError("Invalid subscription data")


class SubscriptionTaskResultEncoder(Encoder[KafkaPayload, SubscriptionTaskResult]):
    def encode(self, value: SubscriptionTaskResult) -> KafkaPayload:
        subscription_id = str(value.task.task.identifier)
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


class SubscriptionScheduledTaskEncoder(Encoder[bytes, ScheduledTask[Subscription]]):
    """
    Encodes/decodes a scheduled subscription to bytes for Kafka.
    Does not support non SnQL subscriptions.
    """

    def __init__(self, entity_key: EntityKey) -> None:
        self.__entity_key = entity_key

    def encode(self, value: ScheduledTask[Subscription]) -> bytes:
        assert isinstance(value.task.data, SnQLSubscriptionData)

        return cast(
            bytes,
            rapidjson.dumps(
                {
                    "timestamp": value.timestamp.isoformat(),
                    "task": {
                        "identifier": str(value.task.identifier),
                        "data": value.task.data.to_dict(),
                    },
                }
            ).encode("utf-8"),
        )

    def decode(self, value: bytes) -> ScheduledTask[Subscription]:
        scheduled_subscription_dict = rapidjson.loads(value.decode("utf-8"))
        assert scheduled_subscription_dict["task"]["data"]["type"] == "snql"
        return ScheduledTask(
            datetime.fromisoformat(scheduled_subscription_dict["timestamp"]),
            Subscription(
                SubscriptionIdentifier.from_string(
                    scheduled_subscription_dict["task"]["identifier"]
                ),
                SnQLSubscriptionData.from_dict(
                    scheduled_subscription_dict["task"]["data"], self.__entity_key
                ),
            ),
        )
