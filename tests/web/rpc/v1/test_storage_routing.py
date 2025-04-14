import uuid
from dataclasses import asdict, dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    Tuple,
    TypeAlias,
    TypeVar,
    Union,
    cast,
)
from unittest.mock import MagicMock, patch

import pytest
import sentry_sdk
from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_sdk.tracing import Span

from snuba import state
from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.downsampled_storage_tiers import Tier
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.request import Request
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc.v1.resolvers.R_eap_items.common.storage_routing import (
    BaseRoutingStrategy,
    RoutedRequestType,
    RoutingContext,
    ClickhouseQuerySettings
)


class RoutingStrategyFailsToSelectTier(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        raise Exception

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        return QueryResult(result=MagicMock(), extra=MagicMock())

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass



class RoutingStrategySelectsTier8(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        return Tier.TIER_8, {}

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        return QueryResult(result=MagicMock(), extra=MagicMock())

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass



class RoutingStrategyUpdatesQuerySettings(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        return Tier.TIER_8, {"some_setting": "some_value"}

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        return QueryResult(result=MagicMock(), extra=MagicMock())

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


def test_target_tier_is_tier_1_if_routing_strategy_fails_to_decide_tier() -> None:
    routing_context = RoutingContext(
        in_msg=MagicMock(spec=RoutedRequestType),
        timer=MagicMock(spec=Timer),
        build_query=MagicMock(),
        query_settings=MagicMock(spec=HTTPQuerySettings),
        target_tier=None,
        query_result=MagicMock(spec=QueryResult),
        extra_info={},
    )
    RoutingStrategyFailsToSelectTier().run_query(routing_context)
    assert routing_context.target_tier == Tier.TIER_1


def test_target_tier_is_set_in_routing_context() -> None:
    routing_context = RoutingContext(
        in_msg=MagicMock(spec=RoutedRequestType),
        timer=MagicMock(spec=Timer),
        build_query=MagicMock(),
        query_settings=MagicMock(spec=HTTPQuerySettings),
        target_tier=None,
        query_result=MagicMock(spec=QueryResult),
        extra_info={},
    )
    RoutingStrategySelectsTier8().run_query(routing_context)
    assert routing_context.target_tier == Tier.TIER_8


def test_merge_query_settings() -> None:
    routing_context = RoutingContext(
        in_msg=MagicMock(spec=RoutedRequestType),
        timer=MagicMock(spec=Timer),
        build_query=MagicMock(),
        query_settings=HTTPQuerySettings(),
        target_tier=None,
        query_result=MagicMock(spec=QueryResult),
        extra_info={},
    )
    RoutingStrategyUpdatesQuerySettings().run_query(routing_context)
    assert routing_context.target_tier == Tier.TIER_8
    assert routing_context.query_settings.get_clickhouse_settings() == {"some_setting": "some_value"}
