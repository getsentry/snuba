import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, TypeAlias, Union

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_sdk.tracing import Span

from snuba import environment
from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.downsampled_storage_tiers import Tier
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
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
ClickhouseQuerySettings = Dict[str, Any]


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
    """
    TODO:

        - built in timings for all stages of the strategy
        - emitting metrics to span and metrics backend
        - standardized way to get parameter values
        - output metrics should log error metrics
    """

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @property
    def metrics(self) -> MetricsWrapper:
        return MetricsWrapper(
            environment.metrics,
            "routing_strategy",
            tags={"routing_strategy_name": self.__class__.__name__},
        )

    def _build_snuba_request(self, routing_context: RoutingContext) -> SnubaRequest:
        request = routing_context.request
        build_query = routing_context.build_query
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
            query_settings=routing_context.query_settings,
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

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        snuba_request = self._build_snuba_request(routing_context)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=routing_context.timer,
        )
        routing_context.query_result = res
        return res

    def __merge_clickhouse_settings(
        self, routing_context: RoutingContext, query_settings: ClickhouseQuerySettings
    ) -> None:
        """merge query settings decided in _decide_tier_and_query_settings with whatever was passed in the
        routing context initially

        the settings in _decide_tier_and_query_settings take priority
        """

        for k, v in query_settings.items():
            routing_context.query_settings.push_clickhouse_setting(k, v)

    def _record_value_in_span_and_DD(
        self,
        span: Span | None,
        metrics_backend_func: MetricsBackendType,
        name: str,
        value: float | int,
        tags: Dict[str, str] | None = None,
    ) -> None:
        name = _SAMPLING_IN_STORAGE_PREFIX + name
        metrics_backend_func(name, value, tags, None)
        if span:
            span.set_data(name, value)

    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        raise NotImplementedError

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        raise NotImplementedError

    def run_query(self, routing_context: RoutingContext) -> QueryResult:
        try:
            routing_context.timer.mark(_START_ESTIMATION_MARK)
            target_tier, query_settings = self._decide_tier_and_query_settings(
                routing_context
            )
            routing_context.timer.mark(_END_ESTIMATION_MARK)
            self._record_value_in_span_and_DD(
                routing_context.metrics_backend.timing,
                "estimation_time_overhead",
                routing_context.timer.get_duration_between_marks(
                    _START_ESTIMATION_MARK, _END_ESTIMATION_MARK
                ),
                {"tier": str(target_tier)},
            )
            routing_context.target_tier = target_tier
            self.__merge_clickhouse_settings(routing_context, query_settings)

        except Exception:
            # log some error metrics
            routing_context.target_tier = Tier.TIER_1

        routing_context.query_settings.set_sampling_tier(routing_context.target_tier)
        output = self._run_query(routing_context)
        routing_context.query_result = output
        try:
            self._output_metrics(routing_context)
        except Exception:
            # log some error metrics
            pass

        return routing_context.query_result
