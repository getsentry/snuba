from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Optional

from confluent_kafka import KafkaError
from confluent_kafka import Message as KafkaMessage
from confluent_kafka import Producer

from snuba import settings
from snuba.attribution import logger, metrics
from snuba.state import safe_dumps
from snuba.utils.streams.configuration_builder import (
    build_default_kafka_producer_configuration,
)
from snuba.utils.streams.topics import Topic

from .appid import AppID

kfk = None


@dataclass(frozen=True)
class QueryAttributionData:
    query_id: str
    table: str
    bytes_scanned: float


@dataclass(frozen=True)
class AttributionData:
    app_id: AppID
    referrer: str
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


def record_attribution(attr_data: AttributionData) -> None:
    global kfk

    log_attribution_to_metrics(attr_data)

    try:
        data = asdict(attr_data)
        data[
            "app_id"
        ] = attr_data.app_id.key  # Don't record the entire AppID, just the key
        data_str = safe_dumps(data)

        if kfk is None:
            kfk = Producer(build_default_kafka_producer_configuration())

        kfk.poll(0)  # trigger queued delivery callbacks
        kfk.produce(
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
