from datetime import UTC, datetime, timedelta
from typing import Any, Dict

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType

from snuba import state
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.downsampled_storage_tiers import Tier
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_trace_item_table import build_query
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.outcomes_based import (
    Outcome,
    OutcomeCategory,
    OutcomesBasedRoutingStrategy,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
)
from tests.helpers import write_raw_unprocessed_events

BASE_TIME = datetime.now(UTC).replace(
    hour=0, minute=0, second=0, microsecond=0
) - timedelta(hours=24)
_PROJECT_ID = 1
_ORG_ID = 1


def _get_request_meta(
    start: datetime | None = None, end: datetime | None = None
) -> RequestMeta:
    start = start or BASE_TIME - timedelta(hours=24)
    end = end or BASE_TIME
    return RequestMeta(
        project_ids=[_PROJECT_ID],
        organization_id=_ORG_ID,
        cogs_category="something",
        referrer="something",
        start_timestamp=Timestamp(seconds=int(start.timestamp())),
        end_timestamp=Timestamp(seconds=int(end.timestamp())),
        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
    )


def gen_span_ingest_outcome(time: datetime, num: int) -> Dict[str, int | str | None]:
    return {
        "org_id": _PROJECT_ID,
        "project_id": _ORG_ID,
        "key_id": None,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "outcome": Outcome.ACCEPTED,
        "reason": None,
        "event_id": None,
        "quantity": num,
        "category": OutcomeCategory.SPAN_INDEXED,
    }


@pytest.fixture
def store_outcomes_data() -> None:
    outcomes_storage = get_storage(StorageKey("outcomes_raw"))
    messages = []
    for hour in range(24):
        time = BASE_TIME - timedelta(hours=hour)
        messages.append(gen_span_ingest_outcome(time, 1_000_000))

    write_raw_unprocessed_events(outcomes_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_outcomes_based_routing_normal_mode(store_outcomes_data: Any) -> None:
    strategy = OutcomesBasedRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL

    routing_context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        build_query=build_query,  # type: ignore
        query_settings=HTTPQuerySettings(),
    )

    tier, settings = strategy._decide_tier_and_query_settings(routing_context)
    assert tier == Tier.TIER_1
    assert settings == {}


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_outcomes_based_routing_downsample(store_outcomes_data: Any) -> None:
    state.set_config(
        "OutcomesBasedRoutingStrategy.max_items_before_downsampling", 5_000_000
    )
    strategy = OutcomesBasedRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL

    routing_context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        build_query=build_query,  # type: ignore
        query_settings=HTTPQuerySettings(),
    )

    tier, settings = strategy._decide_tier_and_query_settings(routing_context)
    assert tier == Tier.TIER_8
    assert settings == {}


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_outcomes_based_routing_metrics_success(store_outcomes_data: Any) -> None:
    state.set_config("OutcomesBasedRoutingStrategy.time_budget_ms", 8000)
    strategy = OutcomesBasedRoutingStrategy()
    request = TraceItemTableRequest(meta=_get_request_meta())
    routing_context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        build_query=build_query,  # type: ignore
        query_settings=HTTPQuerySettings(),
        query_result=QueryResult(
            result={"profile": {"elapsed": 1}},
            extra={"stats": {}, "sql": "", "experiments": {}},
        ),
    )
    routing_context.query_settings.set_sampling_tier(Tier.TIER_1)

    strategy._output_metrics(routing_context)
    assert routing_context.extra_info["sampling_in_storage_routing_success"] == {
        "type": "increment",
        "value": 1,
        "tags": {},
    }


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_outcomes_based_routing_metrics_timeout(store_outcomes_data: Any) -> None:
    state.set_config("OutcomesBasedRoutingStrategy.time_budget_ms", 8000)
    strategy = OutcomesBasedRoutingStrategy()
    request = TraceItemTableRequest(meta=_get_request_meta())
    routing_context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        build_query=build_query,  # type: ignore
        query_settings=HTTPQuerySettings(),
        query_result=QueryResult(
            result={"profile": {"elapsed": 10}},
            extra={"stats": {}, "sql": "", "experiments": {}},
        ),
    )

    strategy._output_metrics(routing_context)
    assert routing_context.extra_info["sampling_in_storage_routing_mistake"] == {
        "type": "increment",
        "value": 1,
        "tags": {"reason": "timeout"},
    }


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_outcomes_based_routing_metrics_sampled_too_low(
    store_outcomes_data: Any,
) -> None:
    state.set_config("OutcomesBasedRoutingStrategy.time_budget_ms", 8000)
    strategy = OutcomesBasedRoutingStrategy()
    request = TraceItemTableRequest(meta=_get_request_meta())
    routing_context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        build_query=build_query,  # type: ignore
        query_settings=HTTPQuerySettings(),
        query_result=QueryResult(
            result={"profile": {"elapsed": 5}},
            extra={"stats": {}, "sql": "", "experiments": {}},
        ),
    )
    routing_context.query_settings.set_sampling_tier(Tier.TIER_8)

    strategy._output_metrics(routing_context)
    assert routing_context.extra_info["sampling_in_storage_routing_mistake"] == {
        "type": "increment",
        "value": 1,
        "tags": {"reason": "sampled_too_low"},
    }
