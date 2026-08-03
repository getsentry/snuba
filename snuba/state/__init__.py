from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import partial
from typing import (
    Any,
)

import simplejson as json
from confluent_kafka import KafkaError, Producer
from confluent_kafka import Message as KafkaMessage
from sentry_kafka_schemas.schema_types import snuba_queries_v1

from snuba import environment, settings
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic

metrics = MetricsWrapper(environment.metrics, "snuba.state")
logger = logging.getLogger("snuba.state")

kfk: Producer | None = None

ratelimit_prefix = "snuba-ratelimit:"

# Rate Limiting and Deduplication

# Window for concurrent query counting
max_query_duration_s = 60
# Window for determining query rate
rate_lookback_s = 60


def _kafka_producer() -> Producer:
    global kfk
    if kfk is None:
        kfk = Producer(
            build_kafka_producer_configuration(
                topic=Topic.QUERYLOG,
            )
        )
    return kfk


# Query Recording


def safe_dumps_default(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {**value}
    raise TypeError(f"Cannot convert object of type {type(value).__name__} to JSON-safe type")


safe_dumps = partial(json.dumps, for_json=True, default=safe_dumps_default)


def _record_query_delivery_callback(error: KafkaError | None, message: KafkaMessage) -> None:
    metrics.increment(
        "record_query.delivery_callback",
        tags={"status": "success" if error is None else "failure"},
    )

    if error is not None:
        logger.warning("Could not record query due to error: %r", error)


def record_query(query_metadata: snuba_queries_v1.Querylog) -> None:
    if not settings.RECORD_QUERIES:
        return

    try:
        producer = _kafka_producer()
        data = safe_dumps(query_metadata)
        producer.poll(0)  # trigger queued delivery callbacks
        producer.produce(
            settings.KAFKA_TOPIC_MAP.get(Topic.QUERYLOG.value, Topic.QUERYLOG.value),
            data.encode("utf-8"),
            on_delivery=_record_query_delivery_callback,
        )
    except Exception as ex:
        logger.exception("Could not record query due to error: %r", ex)


def flush_producer() -> None:
    global kfk
    if kfk is not None:
        messages_remaining = kfk.flush()
        logger.debug(f"{messages_remaining} querylog messages pending delivery")
