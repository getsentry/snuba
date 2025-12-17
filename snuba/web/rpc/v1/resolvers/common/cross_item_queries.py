import uuid
from typing import Any, Literal, overload

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from proto import Message  # type: ignore
from sentry_protos.snuba.v1.endpoint_get_traces_pb2 import GetTracesRequest
from sentry_protos.snuba.v1.request_common_pb2 import (
    RequestMeta,
    TraceItemFilterWithType,
)

from snuba import state
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, or_cond
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import setup_trace_query_settings


def convert_trace_filters_to_trace_item_filter_with_type(
    trace_filters: list[GetTracesRequest.TraceFilter],
) -> list[TraceItemFilterWithType]:
    return [
        TraceItemFilterWithType(item_type=trace_filter.item_type, filter=trace_filter.filter)
        for trace_filter in trace_filters
    ]


@overload
def get_trace_ids_for_cross_item_query(
    original_request: Message,
    request_meta: RequestMeta,
    trace_filters: list[TraceItemFilterWithType],
    timer: Timer,
    return_query_results: Literal[False] = False,
) -> list[str]: ...


@overload
def get_trace_ids_for_cross_item_query(
    original_request: Message,
    request_meta: RequestMeta,
    trace_filters: list[TraceItemFilterWithType],
    timer: Timer,
    return_query_results: Literal[True],
) -> tuple[list[str], list[Any]]: ...


def get_trace_ids_for_cross_item_query(
    original_request: Message,
    request_meta: RequestMeta,
    trace_filters: list[TraceItemFilterWithType],
    timer: Timer,
    return_query_results: bool = False,
) -> list[str] | tuple[list[str], list[Any]]:
    """
    This function is used to get the trace ids that match the given trace filters.
    It does this by creating a query that looks like this:
    SELECT trace_id FROM eap_items
    WHERE (item_type = <item_type_0> AND <trace_item_filter_expression_0>) OR ... OR (item_type = <item_type_n> AND <trace_item_filter_expression_n>)
    GROUP BY trace_id
    HAVING countIf(item_type = <item_type_0> AND <trace_item_filter_expression_0>) > 0 AND ... AND countIf(item_type = <item_type_n> AND <trace_item_filter_expression_n>) > 0

    This works by pruning out items that don't match any of the conditions in the where close. The HAVING
    clause is used to get trace ids that contains items matching all of the conditions.
    """
    # Hacky conversion due to protobuf ugliness
    converted_trace_filters = [trace_filter for trace_filter in trace_filters]
    if isinstance(trace_filters[0], GetTracesRequest.TraceFilter):
        converted_trace_filters = [
            TraceItemFilterWithType(item_type=trace_filter.item_type, filter=trace_filter.filter)
            for trace_filter in trace_filters
        ]

    filter_expressions = []
    for trace_filter in converted_trace_filters:
        filter_expressions.append(
            and_cond(
                f.equals(column("item_type"), trace_filter.item_type),
                trace_item_filters_to_expression(
                    trace_filter.filter,
                    attribute_key_to_expression,
                ),
            )
        )

    if len(filter_expressions) > 1:
        trace_item_filters_and_expression = and_cond(
            *[f.greater(f.countIf(expression), 0) for expression in filter_expressions]
        )
        trace_item_filters_or_expression = or_cond(*filter_expressions)
    else:
        trace_item_filters_and_expression = f.greater(f.countIf(filter_expressions[0]), 0)
        trace_item_filters_or_expression = filter_expressions[0]
    entity = Entity(
        key=EntityKey("eap_items"),
        schema=get_entity(EntityKey("eap_items")).get_data_model(),
        sample=None,
    )
    query = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(
                name="trace_id",
                expression=column("trace_id"),
            )
        ],
        condition=base_conditions_and(
            request_meta,
            trace_item_filters_or_expression,
        ),
        groupby=[
            column("trace_id"),
        ],
        having=trace_item_filters_and_expression,
    )

    treeify_or_and_conditions(query)

    all_confs = state.get_all_configs()
    clickhouse_query_settings = {
        k.split("/", 1)[1]: v
        for k, v in all_confs.items()
        if k.startswith("cross_item_query_settings/")
    }

    query_settings = setup_trace_query_settings() if request_meta.debug else HTTPQuerySettings()

    for key, value in clickhouse_query_settings.items():
        query_settings.push_clickhouse_setting(key, value)

    snuba_request = SnubaRequest(
        id=uuid.UUID(request_meta.request_id),
        original_body=MessageToDict(original_request),
        query=query,
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request_meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request_meta.organization_id,
                "referrer": request_meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_span_samples",
        ),
    )

    results = run_query(
        dataset=PluggableDataset(name="eap", all_entities=[]),
        request=snuba_request,
        timer=timer,
    )
    trace_ids: list[str] = []
    for row in results.result.get("data", []):
        trace_ids.append(list(row.values())[0])

    sentry_sdk.update_current_span(
        attributes={
            "cross_item_query_trace_ids_count": len(trace_ids),
        }
    )

    if return_query_results:
        return trace_ids, [results]
    return trace_ids
