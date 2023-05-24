from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Iterator, Optional

from confluent_kafka import KafkaError
from confluent_kafka import Message as KafkaMessage
from confluent_kafka import Producer

from snuba import settings
from snuba.attribution import logger, metrics
from snuba.state import safe_dumps
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic

from .appid import AppID

kfk: Producer | None = None


@dataclass(frozen=True)
class QueryAttributionData:
    query_id: str
    table: str
    bytes_scanned: float


@dataclass(frozen=True)
class AttributionData:
    app_id: AppID
    referrer: str
    parent_api: str
    request_id: str
    timestamp: int
    duration_ms: int
    dataset: str
    entity: str
    queries: list[QueryAttributionData]


def log_attribution_to_metrics(attr_data: AttributionData) -> None:
    for q in attr_data.queries:
        tags = {
            "app_id": attr_data.app_id.key,
            "referrer": attr_data.referrer,
            "parent_api": attr_data.parent_api,
            "dataset": attr_data.dataset,
            "entity": attr_data.entity,
            "table": q.table,
        }
        metrics.increment("log", q.bytes_scanned, tags=tags)


def _record_attribution_delivery_callback(
    error: Optional[KafkaError], message: KafkaMessage
) -> None:
    metrics.increment(
        "record_attribution.delivery_callback",
        tags={"status": "success" if error is None else "failure"},
    )

    if error is not None:
        logger.warning(
            "Could not record attribution due to error: %r", error, exc_info=True
        )


def _attribution_producer() -> Producer:
    global kfk

    if kfk is None:
        kfk = Producer(build_kafka_producer_configuration(Topic.ATTRIBUTION, None))

    return kfk


@contextmanager
def flush_attribution_producer() -> Iterator[None]:
    global kfk

    try:
        yield
    finally:
        if kfk is not None:
            messages_remaining = kfk.flush()
            logger.debug(f"{messages_remaining} querylog messages pending delivery")


def record_attribution(attr_data: AttributionData) -> None:
    log_attribution_to_metrics(attr_data)

    try:
        data = asdict(attr_data)
        data[
            "app_id"
        ] = attr_data.app_id.key  # Don't record the entire AppID, just the key
        data_str = safe_dumps(data)

        producer = _attribution_producer()
        producer.poll(0)  # trigger queued delivery callbacks
        producer.produce(
            settings.KAFKA_TOPIC_MAP.get(
                Topic.ATTRIBUTION.value, Topic.ATTRIBUTION.value
            ),
            data_str.encode("utf-8"),
            on_delivery=_record_attribution_delivery_callback,
        )
    except Exception as ex:
        logger.warning(
            "Could not record attribution due to error: %r", ex, exc_info=True
        )
