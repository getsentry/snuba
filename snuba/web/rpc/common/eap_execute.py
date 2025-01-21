import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Literal, MutableMapping, Optional

from snuba import settings
from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.query import run_query

eap_executor = ThreadPoolExecutor(max_workers=settings.CLICKHOUSE_MAX_POOL_SIZE)


def run_eap_query(
    dataset: Literal["eap_spans", "spans_str_attrs", "spans_num_attrs"],
    query: Query,
    original_body: dict[str, Any],
    referrer: str,
    organization_id: int,
    parent_api: str,
    timer: Timer,
    clickhouse_settings: Optional[MutableMapping[str, Any]] = None,
) -> QueryResult:
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )
    query_settings = HTTPQuerySettings()
    if clickhouse_settings is not None:
        query_settings.set_clickhouse_settings(clickhouse_settings)

    query.set_from_clause(entity)

    request = Request(
        id=uuid.uuid4(),
        original_body=original_body,
        query=query,
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": organization_id,
                "referrer": referrer,
            },
            app_id=AppID("eap"),
            parent_api=parent_api,
        ),
    )

    return run_query(PluggableDataset(name=dataset, all_entities=[]), request, timer)


def run_eap_query_async(
    dataset: Literal["eap_spans", "spans_str_attrs", "spans_num_attrs"],
    query: Query,
    original_body: dict[str, Any],
    referrer: str,
    organization_id: int,
    parent_api: str,
    timer: Timer,
    clickhouse_settings: Optional[MutableMapping[str, Any]] = None,
) -> Future[QueryResult]:
    return eap_executor.submit(
        run_eap_query,
        dataset=dataset,
        query=query,
        original_body=original_body,
        referrer=referrer,
        organization_id=organization_id,
        parent_api=parent_api,
        timer=timer,
        clickhouse_settings=clickhouse_settings,
    )
