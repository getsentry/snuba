from typing import Callable

from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest

from snuba import state
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
from snuba.web import QueryResult
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.load_retriever import (
    get_cluster_loadinfo,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategy_selector import (
    RoutingStrategySelector,
)


@with_span(op="function")
def run_query_to_correct_tier(
    in_msg: TraceItemTableRequest | TimeSeriesRequest,
    query_settings: HTTPQuerySettings,
    timer: Timer,
    build_query: Callable[[TraceItemTableRequest | TimeSeriesRequest], Query],
) -> QueryResult:
    routing_context = RoutingContext(
        in_msg=in_msg,
        timer=timer,
        build_query=build_query,
        query_settings=query_settings,
    )

    # we're calling this function to get the cluster load info to emit metrics and to prevent dead code
    # the result is currently not used in storage routing
    # can turn off on Snuba Admin
    if state.get_config("storage_routing.enable_get_cluster_loadinfo", True):
        get_cluster_loadinfo()

    selected_strategy = RoutingStrategySelector().select_routing_strategy(
        routing_context
    )
    return selected_strategy.run_query_to_correct_tier(routing_context)
