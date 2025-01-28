import uuid
from collections import OrderedDict
from typing import Any, Dict, Iterable, Tuple

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    StatsDataPoint,
    TraceItemResult,
    TraceItemStats,
    TraceItemStatsRequest,
    TraceItemStatsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import LimitBy, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import arrayJoin, column, tupleElement
from snuba.query.expressions import FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.constants import ATTRIBUTE_BUCKETS
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.v1.endpoint_get_traces import _DEFAULT_ROW_LIMIT
from snuba.web.rpc.v1.resolvers import ResolverTraceItemStats
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.aggregation import (
    aggregation_to_expression,
)

MAX_LIMIT_KEYS_BY = 50
DEFAULT_LIMIT_KEYS_BY = 5


def _transform_results(
    aggregation: AttributeAggregation,
    results: Iterable[Dict[str, Any]],
) -> Iterable[StatsDataPoint]:

    # Maintain the order of keys, so it is in descending order
    # of most prevelant key-value pair. Say the following data
    # is returned:
    # data = [
    #   ('sdk.name', 'python', 100),
    #   ('server_name', 'DW9H09PDFM.local', 80),
    #   ('sdk.name', 'javascript', 50),
    #   ('messaging.system', 'redis', 20)
    # ]
    #
    # the order returned will be
    # [
    #   {"attribute_key": "sdk.name", "data": [{"label":"python", "value": 100}, {"label":"javascript", "value": 50}]},
    #   {"attribute_key": "server_name", "data": [{"label":"DW9H09PDFM.local", "value": 90}]},
    #   {"attribute_key": "messaging.system", "data": [{"label":"redis", "value": 20}]},
    # ]
    res: OrderedDict[Tuple[str, str], Iterable[StatsDataPoint]] = OrderedDict()

    label = aggregation.label

    for row in results:
        attr_key = row["attr_key"]
        attr_value = row["attr_value"]
        default = StatsDataPoint(
            key=AttributeKey(name=attr_key, type=AttributeKey.Type.TYPE_STRING),
            aggregation=label,
        )
        res.setdefault((attr_key, label), default).data.append(
            StatsDataPoint.AttributeResults(label=attr_value, value=row[label])
        )

    return list(res.values())


def _build_stats_snuba_request(
    request: TraceItemStatsRequest, query: Query
) -> SnubaRequest:
    query_settings = (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=query,
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_attribute_stats",
        ),
    )


def _build_stats_query(
    in_msg: TraceItemStatsRequest, aggregation: AttributeAggregation
):
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )

    # TODO: Add this use case to hash_bucket_functions.py?
    # TODO: Support arrayjoin_optimizer?
    concat_attr_maps = FunctionCall(
        alias="attr_str_concat",
        function_name="mapConcat",
        parameters=tuple(column(f"attr_str_{i}") for i in range(ATTRIBUTE_BUCKETS)),
    )
    attrs_string_keys = tupleElement(
        "attr_key", arrayJoin("attr_str", concat_attr_maps), Literal(None, 1)
    )
    attrs_string_values = tupleElement(
        "attr_value",
        arrayJoin("attr_str", concat_attr_maps),
        Literal(None, 2),
    )

    selected_columns = [
        SelectedExpression(
            name="attr_key",
            expression=attrs_string_keys,
        ),
        SelectedExpression(
            name="attr_value",
            expression=attrs_string_values,
        ),
        SelectedExpression(
            name=aggregation.label,
            expression=aggregation_to_expression(aggregation),
        ),
    ]

    trace_item_filters_expression = trace_item_filters_to_expression(in_msg.filter)

    query = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            in_msg.meta,
            trace_item_filters_expression,
        ),
        order_by=[
            OrderBy(
                direction=OrderByDirection.DESC,
                expression=aggregation_to_expression(aggregation),
            ),
        ],
        groupby=[
            attrs_string_keys,
            attrs_string_values,
        ],
        limitby=LimitBy(
            limit=in_msg.limit_keys_by,
            columns=[attrs_string_keys],
        ),
        limit=in_msg.limit if in_msg.limit > 0 else _DEFAULT_ROW_LIMIT,
    )

    return query


class ResolverTraceItemStatsEAPSpans(ResolverTraceItemStats):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_SPAN

    def resolve(self, in_msg: TraceItemStatsRequest) -> TraceItemStatsResponse:
        # Hardcoding the aggregation for now to keep the endpoint simple - although
        # this can easily be ported over to TraceItemStatsRequest. If we expose aggregations,
        # we'll probably want to expose OrderBy too. Order by currently has a loaded
        # meaning in this endpoint. It is both how we order the key-value pair results
        # in our ClickHouse query and subsequently, the attribute keys in the final response.
        # Since we only have a single use case right now, the complication from a
        # user-defined order by is not worth tackling in the first pass.
        aggregation = AttributeAggregation(
            aggregate=Function.FUNCTION_COUNT,
            key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="sentry.duration_ms"),
            label="count(span.duration)",
            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
        )

        stats = TraceItemResult()
        if TraceItemStatsRequest.Type.TYPE_STATS in in_msg.types:
            query = _build_stats_query(in_msg, aggregation)
            treeify_or_and_conditions(query)
            snuba_request = _build_stats_snuba_request(in_msg, query)

            results = run_query(
                dataset=PluggableDataset(name="eap", all_entities=[]),
                request=snuba_request,
                timer=self._timer,
            )

            data_points = _transform_results(
                aggregation, results.result.get("data", [])
            )
            stats.stats.CopyFrom(TraceItemStats(data_points=data_points))

        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [],
            [self._timer],
        )

        results = []
        if TraceItemStatsRequest.Type.TYPE_STATS in in_msg.types:
            results.append(stats)

        return TraceItemStatsResponse(
            results=results,
            meta=response_meta,
        )
