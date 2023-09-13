from __future__ import annotations

import logging
import time
from collections import deque
from concurrent.futures import Future
from enum import Enum
from json import dumps
from math import floor
from typing import Any, Deque, Mapping, MutableMapping, NamedTuple, Optional, Sequence

from arroyo.backends.abstract import Producer
from arroyo.backends.kafka.configuration import build_kafka_configuration
from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.types import BrokerValue, Topic

from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic as StreamTopic


class UsageUnit(Enum):
    MILLISECONDS = "milliseconds"
    BYTES = "bytes"
    MILLISECONDS_SEC = "milliseconds_sec"


class UsageKey(NamedTuple):
    timestamp: int
    resource_id: str
    app_feature: str
    unit: UsageUnit


DEFAULT_TOPIC_NAME = "shared-resources-usage"
DEFAULT_QUEUE_SIZE = 9500
CLOSE_TIMEOUT_SEC = 60

logger = logging.getLogger("usageaccountant")


class KafkaConfig(NamedTuple):
    bootstrap_servers: Sequence[str]
    config_params: Mapping[str, Any]


class UsageAccumulator:
    """
    Records the usage of shared resources. This library is meant to
    produce observability data on how much shared resources are used
    by different features.

    Data is accumulated locally and produced to Kafka periodically
    to reduce the volume impact on Kafka when a large number of pods
    use this api.

    In order for accumulation to be effective and preserve the Kafka
    consumers that will consume from this, instances of this should be
    shared and kept around for as long as possible.

    A good idea is to create an instance at the beginning of your
    program and keep reusing that till you exit.

    Resources are identified as `resource_id` and feature as
    `app_feature` in the methods. These cannot be enum otherwise we would
    have to re-release the api every time we add one. And we do not
    have a single source of truth for them anyway.
    """

    def __init__(
        self,
        granularity_sec: int = 60,
        topic_name: str = DEFAULT_TOPIC_NAME,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        kafka_config: Optional[KafkaConfig] = None,
        producer: Optional[Producer[KafkaPayload]] = None,
    ) -> None:
        """
        Initializes the accumulator. Instances should be kept around
        as much as possible as they preserve the instance of the
        Kafka producers, with it the connections.

        `granularity_sec` defines how often the accumulator will be
        flushed.
        """
        self.__first_timestamp: Optional[float] = None
        self.__usage_batch: MutableMapping[UsageKey, int] = {}
        self.__granularity_sec = granularity_sec

        self.__topic = Topic(topic_name)

        if producer is not None:
            assert kafka_config is None, (
                "If producer is provided, initialization "
                "parameters cannot be provided"
            )
            self.__producer: Producer[KafkaPayload] = producer

        else:
            assert kafka_config is not None, (
                "If no producer is provided, initialization parameters "
                "have to be provided"
            )

            self.__producer = KafkaProducer(
                build_kafka_configuration(
                    default_config=kafka_config.config_params,
                    bootstrap_servers=kafka_config.bootstrap_servers,
                )
            )

        self.__queue_size = queue_size
        self.__futures: Deque[Future[BrokerValue[KafkaPayload]]] = deque()

        self.__last_log: Optional[float] = None

    def record(
        self,
        resource_id: str,
        app_feature: str,
        amount: int,
        usage_type: UsageUnit,
    ) -> None:
        """
        Record a chunk of usage of a resource for a feature.

        This method is not a blocking one and it is cheap on
        the main thread. So feel free to call it often.
        Specifically it takes the system time to associate to
        the usage to a time range.

        `resource_id` identifies the shared resource.
          Example: `generic_metrics_indexer_ingestion`
        `app_feature` identifies the product feature.
        `amount`  is the amount of resource used.
        `usage_type` is the unit of measure for `amount`.
        """
        now = time.time()
        if (
            self.__first_timestamp is not None
            and now - self.__first_timestamp >= self.__granularity_sec
        ):
            self.flush()
            self.__first_timestamp = now

        key = UsageKey(
            floor(now / self.__granularity_sec) * 10,
            resource_id,
            app_feature,
            usage_type,
        )

        if (
            key not in self.__usage_batch
            and len(self.__usage_batch) >= self.__queue_size
        ):
            # Avoid logging too often. Logging very often can become a major
            # overhead for the call site. This function can be called in tight
            # loops or in very high throughput scenarios. We should err on the
            # side of safety.
            if not self.__last_log or self.__last_log < now - 1:
                self.__last_log = now
                logger.error("Buffer overflow in the usage accountant. Max length")
                self.flush()
            return

        if self.__first_timestamp is None:
            self.__first_timestamp = now

        if key not in self.__usage_batch:
            self.__usage_batch[key] = amount
        else:
            self.__usage_batch[key] += amount

    def flush(self) -> None:
        """
        This method is blocking and it forces the api to flush
        data accumulated to Kafka.

        This method is supposed to be used when we are shutting
        down the program that was accumulating data.
        """
        while self.__futures and self.__futures[0].done():
            try:
                self.__futures[0].result()
            except Exception as e:
                logger.exception(e)
            self.__futures.popleft()

        for key, amount in self.__usage_batch.items():
            if len(self.__futures) >= self.__queue_size:
                logger.error(
                    (
                        "Too many Kafka messages pending callback. Clearing "
                        "the buffer. Usage data was dropped."
                    )
                )
                break

            message = {
                "timestamp": key.timestamp,
                "shared_resource_id": key.resource_id,
                "app_feature": key.app_feature,
                "usage_unit": key.unit.value,
                "amount": amount,
            }

            result = self.__producer.produce(
                self.__topic,
                KafkaPayload(
                    key=None, value=dumps(message).encode("utf-8"), headers=[]
                ),
            )
            self.__futures.append(result)

        self.__usage_batch.clear()

    def close(self) -> None:
        result = self.__producer.close()
        try:
            result.result(timeout=CLOSE_TIMEOUT_SEC)
        except Exception as e:
            logger.exception(e)


# TODO: Remove almost everything above once https://github.com/getsentry/usage-accountant
# is published and import the UsageAccumulator from there instead

accumulator: UsageAccumulator | None = None


def _accumulator() -> UsageAccumulator:
    global accumulator
    if accumulator is None:
        producer = KafkaProducer(
            build_kafka_producer_configuration(
                StreamTopic.COGS_SHARED_RESOURCES_USAGE, None
            )
        )
        accumulator = UsageAccumulator(producer=producer)
    return accumulator


def record_cogs(
    resource_id: str, app_feature: str, amount: int, usage_type: UsageUnit
) -> None:
    try:
        accumulator = _accumulator()
        accumulator.record(resource_id, app_feature, amount, usage_type)
    except Exception as err:
        logger.warning("Could not record COGS due to error: %r", err, exc_info=True)
