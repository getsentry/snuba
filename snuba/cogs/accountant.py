from __future__ import annotations

import logging

from arroyo.backends.kafka.consumer import KafkaProducer
from usageaccountant import UsageAccumulator, UsageUnit

from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic as StreamTopic

logger = logging.getLogger("usageaccountant")


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
