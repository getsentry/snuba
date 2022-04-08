from __future__ import annotations

from dataclasses import dataclass

from snuba.attribution import metrics

from .appid import AppID


@dataclass(frozen=True)
class QueryAttributionData:
    query_id: str
    table: str
    bytes_scanned: float


@dataclass(frozen=True)
class AttributionData:
    app_id: AppID
    referrer: str
    dataset: str
    entity: str
    queries: list[QueryAttributionData]


def log_attribution(attr_data: AttributionData) -> None:
    for q in attr_data.queries:
        tags = {
            "app_id": attr_data.app_id.key,
            "referrer": attr_data.referrer,
            "dataset": attr_data.dataset,
            "entity": attr_data.entity,
            "table": q.table,
        }
        metrics.increment("log", q.bytes_scanned, tags=tags)
