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
    TypedDict
)

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
from snuba.utils.registered_class import RegisteredClass
from snuba.web import QueryResult
from snuba.web.query import run_query

_SAMPLING_IN_STORAGE_PREFIX = "sampling_in_storage_"
_START_ESTIMATION_MARK = "start_sampling_in_storage_estimation"
_END_ESTIMATION_MARK = "end_sampling_in_storage_estimation"
MetricsBackendType: TypeAlias = Callable[
    [str, Union[int, float], Optional[Dict[str, str]], Optional[str]], None
]

RoutedRequestType = Union[TimeSeriesRequest, TraceItemTableRequest]
ClickhouseQuerySettings = Dict[str, str]


def _build_snuba_request(
    request: RoutedRequestType,
    query_settings: QuerySettings,
    build_query: Callable[[RoutedRequestType], Query],
) -> SnubaRequest:
    if request.meta.trace_item_type == TraceItemType.TRACE_ITEM_TYPE_LOG:
        team = "ourlogs"
        feature = "ourlogs"
        parent_api = "ourlog_trace_item_table"
    else:
        team = "eap"
        feature = "eap"
        parent_api = "eap_span_samples"

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=build_query(request),
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team=team,
            feature=feature,
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api=parent_api,
        ),
    )


@dataclass
class RoutingContext:
    # NIT: some things should only be settable once
    # e.g. query_result
    in_msg: RoutedRequestType
    timer: Timer
    build_query: Callable[[TimeSeriesRequest | TraceItemTableRequest], Query]
    query_settings: HTTPQuerySettings
    target_tier: Tier | None  # TODO (make default Tier 1??)
    query_result: QueryResult | None
    # NOTE: this is a landmine
    extra_info: dict[str, Any]


class BaseRoutingStrategy(metaclass=RegisteredClass):
    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        raise NotImplementedError

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        snuba_request = _build_snuba_request(
            routing_context.in_msg,
            routing_context.query_settings,
            routing_context.build_query,
        )
        return run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=routing_context.timer,
        )

    def __merge_query_settings(
        self, routing_context: RoutingContext, query_settings: ClickhouseQuerySettings
    ) -> None:
        """merge query settings decided in _decide_tier_and_query_settings with whatever was passed in the
        routing context initially

        the settings in _decide_tier_and_query_settings take priority
        """
        for k, v in query_settings.items():
            routing_context.query_settings.push_clickhouse_setting(k, v)

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        raise NotImplementedError

    def run_query(self, routing_context: RoutingContext) -> QueryResult:
        try:
            target_tier, query_settings = self._decide_tier_and_query_settings(
                routing_context
            )
            routing_context.target_tier = target_tier
            self.__merge_query_settings(routing_context, query_settings)
        except Exception:
            # log some error metrics
            routing_context.target_tier = Tier.TIER_1

        output = self._run_query(routing_context)
        routing_context.query_result = output
        try:
            self._output_metrics(routing_context)
        except Exception:
            # log some error metrics
            pass

        return routing_context.query_result
