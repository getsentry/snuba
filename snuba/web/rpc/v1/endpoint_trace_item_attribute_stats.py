import uuid
from typing import Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_stats_pb2 import (
    TraceItemAttributesStatsRequest,
    TraceItemAttributesStatsResponse,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import Function

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
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.endpoint_get_traces import _DEFAULT_ROW_LIMIT
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.aggregation import (
    aggregation_to_expression,
)


def _build_snuba_request(
    request: TraceItemAttributesStatsRequest, query: Query
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


def _validate_aggregation_attributes(in_msg: TraceItemAttributesStatsRequest):
    if not in_msg.aggregations or len(in_msg.aggregations) == 0:
        raise BadSnubaRPCRequestException("At least one aggregation required.")

    # For now, just restrict it to count
    if len(in_msg.aggregations) > 1:
        raise BadSnubaRPCRequestException("Multiple aggregations not supported.")

    aggregation = in_msg.aggregations[0]
    if (
        aggregation.aggregate != Function.FUNCTION_COUNT
        or aggregation.key.name != "sentry.duration_ms"
    ):
        raise BadSnubaRPCRequestException(
            "Only count(sentry.duration_ms) is supported."
        )


class EndpointTraceItemAttributeStats(
    RPCEndpoint[TraceItemAttributesStatsRequest, TraceItemAttributesStatsResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TraceItemAttributesStatsRequest]:
        return TraceItemAttributesStatsRequest

    @classmethod
    def response_class(cls) -> Type[TraceItemAttributesStatsResponse]:
        return TraceItemAttributesStatsResponse

    def _execute(
        self, in_msg: TraceItemAttributesStatsRequest
    ) -> TraceItemAttributesStatsResponse:

        in_msg.meta.request_id = getattr(in_msg.meta, "request_id", None) or str(
            uuid.uuid4()
        )

        _validate_aggregation_attributes(in_msg)

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
        ]

        selected_columns.extend(
            [
                SelectedExpression(
                    name="count",
                    expression=aggregation_to_expression(agg),
                )
                for agg in in_msg.aggregations
            ]
        )

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
                    expression=aggregation_to_expression(in_msg.aggregations[0]),
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

        treeify_or_and_conditions(query)

        snuba_request = _build_snuba_request(in_msg, query)

        results = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )

        # TODO: Transform results

        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [],
            [self._timer],
        )

        return TraceItemAttributesStatsResponse(
            result_stats=[],
            meta=response_meta,
        )
