import uuid

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
from snuba.downsampled_storage_tiers import Tier
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, or_cond
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)

_TRACE_LIMIT = 10000


def convert_trace_filters_to_trace_item_filter_with_type(
    trace_filters: list[GetTracesRequest.TraceFilter],
) -> list[TraceItemFilterWithType]:
    return [
        TraceItemFilterWithType(item_type=trace_filter.item_type, filter=trace_filter.filter)
        for trace_filter in trace_filters
    ]


def get_trace_ids_sql_for_cross_item_query(
    original_request: Message,
    request_meta: RequestMeta,
    trace_filters: list[TraceItemFilterWithType],
    sampling_tier: Tier,
    timer: Timer,
    limit: int | None = None,
) -> tuple[str, QueryResult]:
    """
    Returns the SQL query string and query result for getting trace IDs matching the given filters.
    This allows the query to be used as a subquery in subsequent queries.

    This function builds the same query as get_trace_ids_for_cross_item_query() but
    returns the SQL string instead of executing it. Uses dry_run mode to get the SQL
    without actually querying ClickHouse.

    Returns:
        tuple: (sql_string, query_result) where query_result contains metadata like sampling_tier
    """
    filter_expressions = []
    if trace_filters:
        converted_trace_filters = [trace_filter for trace_filter in trace_filters]
        if isinstance(trace_filters[0], GetTracesRequest.TraceFilter):
            converted_trace_filters = [
                TraceItemFilterWithType(
                    item_type=trace_filter.item_type, filter=trace_filter.filter
                )
                for trace_filter in trace_filters
            ]

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
    elif len(filter_expressions) == 1:
        trace_item_filters_and_expression = None
        trace_item_filters_or_expression = filter_expressions[0]
    else:
        trace_item_filters_and_expression = None
        trace_item_filters_or_expression = None

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
            *[trace_item_filters_or_expression] if trace_item_filters_or_expression else [],
        ),
        groupby=[
            column("organization_id"),
            column("project_id"),
            column("trace_id"),
        ],
        having=trace_item_filters_and_expression,
        order_by=[
            OrderBy(
                direction=OrderByDirection.DESC,
                expression=column("organization_id"),
            ),
            OrderBy(
                direction=OrderByDirection.DESC,
                expression=column("project_id"),
            ),
            OrderBy(
                direction=OrderByDirection.DESC,
                expression=f.max(column("timestamp")),
            ),
        ],
        limit=limit or state.get_config("trace_ids_cross_item_query_limit", _TRACE_LIMIT),
    )

    treeify_or_and_conditions(query)

    # Use dry_run to get SQL without executing
    query_settings = HTTPQuerySettings(dry_run=True)
    query_settings.set_sampling_tier(sampling_tier)
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

    # Dry-run queries don't populate sampling_tier in stats, so add it manually
    # This is needed for _construct_meta_if_downsampled() to detect downsampled queries
    if "stats" not in results.extra:
        results.extra["stats"] = {}
    results.extra["stats"]["sampling_tier"] = sampling_tier

    # Return both SQL and query result (needed for sampling_tier metadata)
    return results.extra["sql"], results
