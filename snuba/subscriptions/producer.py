from snuba.utils.streams.kafka import KafkaProducer
from snuba.subscriptions.codecs import SubscriptionResult


class SubscriptionResultProducer(KafkaProducer[SubscriptionResult]):
    pass
