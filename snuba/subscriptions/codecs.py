import json

from arroyo.backends.kafka import KafkaPayload

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.query.exceptions import InvalidQueryException
from snuba.subscriptions.data import (
    DelegateSubscriptionData,
    InvalidSubscriptionError,
    LegacySubscriptionData,
    SnQLSubscriptionData,
    SubscriptionData,
)
from snuba.subscriptions.entity_subscription import SubscriptionType
from snuba.subscriptions.worker import SubscriptionTaskResult
from snuba.utils.codecs import Codec, Encoder


class SubscriptionDataCodec(Codec[bytes, SubscriptionData]):
    dataset_name: str

    def __init__(self, dataset: Dataset):
        super().__init__()
        self.dataset_name = get_dataset_name(dataset)

    def encode(self, value: SubscriptionData) -> bytes:
        return json.dumps(value.to_dict()).encode("utf-8")

    def decode(self, value: bytes) -> SubscriptionData:
        try:
            data = json.loads(value.decode("utf-8"))
        except json.JSONDecodeError:
            raise InvalidQueryException("Invalid JSON")

        subscription_type = data.get(SubscriptionData.TYPE_FIELD)
        if subscription_type == SubscriptionType.SNQL.value:
            return SnQLSubscriptionData.from_dict(data, self.dataset_name)
        elif subscription_type == SubscriptionType.DELEGATE.value:
            return DelegateSubscriptionData.from_dict(data, self.dataset_name)
        elif subscription_type is None:
            return LegacySubscriptionData.from_dict(data, self.dataset_name)
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
