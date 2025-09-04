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
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
    Outcome,
    OutcomeCategory,
    OutcomesBasedRoutingStrategy,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
)
from tests.helpers import write_raw_unprocessed_events

BASE_TIME = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
    hours=24
)
_PROJECT_ID = 1
_ORG_ID = 1


def _get_request_meta(
    start: datetime | None = None,
    end: datetime | None = None,
    hour_interval: int | None = None,
    downsampled_storage_config: DownsampledStorageConfig | None = None,
) -> RequestMeta:
    hour_interval = hour_interval or 24
    start = start or BASE_TIME - timedelta(hours=hour_interval)
    end = end or BASE_TIME
    return RequestMeta(
        project_ids=[_PROJECT_ID],
        organization_id=_ORG_ID,
        cogs_category="something",
        referrer="something",
        start_timestamp=Timestamp(seconds=int(start.timestamp())),
        end_timestamp=Timestamp(seconds=int(end.timestamp())),
        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        downsampled_storage_config=downsampled_storage_config,
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

    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
        )
    )
    assert routing_decision.tier == Tier.TIER_1
    assert routing_decision.clickhouse_settings == {}
    assert routing_decision.can_run


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_outcomes_based_routing_downsample(store_outcomes_data: Any) -> None:
    state.set_config("OutcomesBasedRoutingStrategy.max_items_before_downsampling", 5_000_000)
    strategy = OutcomesBasedRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL

    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
        )
    )
    assert routing_decision.tier == Tier.TIER_8
    assert routing_decision.clickhouse_settings == {}
    assert routing_decision.can_run
    state.set_config("OutcomesBasedRoutingStrategy.max_items_before_downsampling", 500_000)
    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
        )
    )
    assert routing_decision.tier == Tier.TIER_64
    assert routing_decision.clickhouse_settings == {}
    assert routing_decision.can_run

    state.set_config("OutcomesBasedRoutingStrategy.max_items_before_downsampling", 50_000)
    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
        )
    )
    assert routing_decision.tier == Tier.TIER_512
    assert routing_decision.clickhouse_settings == {}
    assert routing_decision.can_run


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_outcomes_based_routing_highest_accuracy_mode(store_outcomes_data: Any) -> None:
    strategy = OutcomesBasedRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_HIGHEST_ACCURACY
    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
        )
    )

    assert routing_decision.tier == Tier.TIER_1
    assert routing_decision.clickhouse_settings == {}
    assert routing_decision.can_run


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_outcomes_based_routing_defaults_to_spans_for_unspecified_item_type(
    store_outcomes_data: Any,
) -> None:
    strategy = OutcomesBasedRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.trace_item_type = TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED
    state.set_config("OutcomesBasedRoutingStrategy.max_items_before_downsampling", 50_000)
    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
        )
    )
    assert routing_decision.tier == Tier.TIER_512
    assert routing_decision.clickhouse_settings == {}
    assert routing_decision.can_run
