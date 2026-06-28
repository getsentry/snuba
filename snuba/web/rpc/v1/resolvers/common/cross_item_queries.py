import uuid

from google.protobuf.json_format import MessageToDict
from proto import Message  # type: ignore[import-untyped]
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
from snuba.query.expressions import DangerousRawSQL, Expression
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
    use_array_map_columns,
)

# 50 million trace ids * 16 bytes per id = a limit of 1gigabyte memory usage per cross item query
# most queries do not hit this number this is just an upper bound
_TRACE_LIMIT = 50_000_000

# ``distributed_product_mode='local'`` pushes the cross-item ``trace_id IN (subquery)``
# join down to the local storage nodes. Because eap_items is sharded by ``trace_id``,
# running the join locally on each shard lets ClickHouse use the ``trace_id``
# bloom-filter index to skip scanning large amounts of data, instead of materializing
# a temporary table of trace ids on the distributed (query) node. See EAP-377.
LOCAL_JOIN_DISTRIBUTED_PRODUCT_MODE = "local"


def apply_cross_item_outer_query_settings(
    query_settings: HTTPQuerySettings,
    has_trace_filters: bool,
    sampling_tier: Tier,
) -> None:
    """Apply the ClickHouse settings for the outer query of a (potentially) cross-item
    query. Shared by all EAP resolvers so the logic lives in one place.

    For cross-item queries (``has_trace_filters``):
    - skip sampling on the outer query when ``cross_item_queries_no_sample_outer`` is
      set — the inner trace-ids query is sampled, the outer one should not be;
    - set ``distributed_product_mode='local'`` so the ``trace_id`` join runs locally
      on each shard and can use the bloom-filter index (see EAP-377).

    For non-cross-item queries, the sampling tier is applied as usual.
    """
    cross_item_queries_no_sample_outer = state.get_int_config(
        "cross_item_queries_no_sample_outer", 1
    )
    if not (has_trace_filters and cross_item_queries_no_sample_outer):
        query_settings.set_sampling_tier(sampling_tier)
    if has_trace_filters:
        query_settings.push_clickhouse_setting(
            "distributed_product_mode", LOCAL_JOIN_DISTRIBUTED_PRODUCT_MODE
        )


def trace_id_in_subquery_condition(trace_ids_sql: str) -> Expression:
    """Build the ``trace_id IN (<subquery>)`` condition for the outer query of a
    cross-item query (see EAP-377).

    The condition is emitted as raw SQL referencing the bare ``trace_id`` UUID
    column. This keeps the column out of the ``UUIDColumnProcessor`` (so it is not
    wrapped in ``replaceAll(toString(trace_id), '-', '')``, which would defeat the
    bloom-filter index) and out of the ``PrewhereProcessor`` (so it stays in
    ``WHERE`` rather than ``PREWHERE``, which is incompatible with
    ``distributed_product_mode='local'`` due to a ClickHouse bug). The subquery
    itself selects the bare ``trace_id`` (see
    ``get_trace_ids_sql_for_cross_item_query``) so both sides of the comparison are
    raw UUIDs.
    """
    return DangerousRawSQL(None, f"trace_id IN ({trace_ids_sql})")


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
    # filter_expressions feed the WHERE or_cond (keep the prepared IN-sets for
    # partition/primary-key pruning); having_filter_expressions feed the HAVING countIf
    # (a SELECT-clause aggregate), where the membership must be has(array, x) so its
    # result-block column name is stable across mixed-version ClickHouse nodes
    # (membership_as_has, see common._in_or_has).
    filter_expressions = []
    having_filter_expressions = []
    if trace_filters:
        converted_trace_filters = list(trace_filters)
        if isinstance(trace_filters[0], GetTracesRequest.TraceFilter):
            converted_trace_filters = [
                TraceItemFilterWithType(
                    item_type=trace_filter.item_type, filter=trace_filter.filter
                )
                for trace_filter in trace_filters
            ]

        for trace_filter in converted_trace_filters:
            item_type_cond = f.equals(column("item_type"), trace_filter.item_type)
            filter_expressions.append(
                and_cond(
                    item_type_cond,
                    trace_item_filters_to_expression(
                        trace_filter.filter,
                        attribute_key_to_expression,
                        use_array_map_columns=use_array_map_columns(request_meta),
                    ),
                )
            )
            having_filter_expressions.append(
                and_cond(
                    item_type_cond,
                    trace_item_filters_to_expression(
                        trace_filter.filter,
                        attribute_key_to_expression,
                        membership_as_has=True,
                        use_array_map_columns=use_array_map_columns(request_meta),
                    ),
                )
            )

    if len(filter_expressions) > 1:
        trace_item_filters_and_expression = and_cond(
            *[f.greater(f.countIf(expression), 0) for expression in having_filter_expressions]
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
    # Select the bare ``trace_id`` UUID (as raw SQL, bypassing the UUIDColumnProcessor)
    # instead of the dash-stripped ``replaceAll(toString(trace_id), '-', '')`` form so
    # the outer ``trace_id IN (subquery)`` comparison is UUID-to-UUID and can use the
    # bloom-filter index. The SELECT and GROUP BY expressions must match, so both use
    # the same bare column. See EAP-377.
    trace_id_expression: Expression = DangerousRawSQL(None, "trace_id")
    query = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(
                name="trace_id",
                expression=trace_id_expression,
            )
        ],
        condition=base_conditions_and(
            request_meta,
            *[trace_item_filters_or_expression] if trace_item_filters_or_expression else [],
        ),
        groupby=[
            column("organization_id"),
            column("project_id"),
            trace_id_expression,
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
