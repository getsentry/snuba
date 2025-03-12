import uuid
from typing import Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeValuesRequest,
    TraceItemAttributeValuesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal, literals_array
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    base_conditions_and,
    convert_filter_offset,
    treeify_or_and_conditions,
    truncate_request_meta_to_day,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.legacy.attributes_common import should_use_items_attrs
from snuba.web.rpc.v1.legacy.trace_item_attribute_values import (
    build_snuba_request as build_snuba_request_legacy,
)


def _build_base_conditions_and(request: TraceItemAttributeValuesRequest) -> Expression:
    if request.value_substring_match:
        return (
            base_conditions_and(
                request.meta,
                f.equals(column("attr_key"), literal(request.key.name)),
                # multiSearchAny has special treatment with ngram bloom filters
                # https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#functions-support
                f.multiSearchAny(
                    column("attr_value"),
                    literals_array(None, [literal(request.value_substring_match)]),
                ),
                convert_filter_offset(request.page_token.filter_offset),
            )
            if request.page_token.HasField("filter_offset")
            else base_conditions_and(
                request.meta,
                f.equals(column("attr_key"), literal(request.key.name)),
                f.multiSearchAny(
                    column("attr_value"),
                    literals_array(None, [literal(request.value_substring_match)]),
                ),
            )
        )
    else:
        return (
            base_conditions_and(
                request.meta,
                f.equals(column("attr_key"), literal(request.key.name)),
                convert_filter_offset(request.page_token.filter_offset),
            )
            if request.page_token.HasField("filter_offset")
            else base_conditions_and(
                request.meta,
                f.equals(column("attr_key"), literal(request.key.name)),
            )
        )


def _build_query(request: TraceItemAttributeValuesRequest) -> Query:
    if request.limit > 1000:
        raise BadSnubaRPCRequestException("Limit can be at most 1000")

    entity = Entity(
        key=EntityKey("spans_str_attrs"),
        schema=get_entity(EntityKey("spans_str_attrs")).get_data_model(),
        sample=None,
    )

    truncate_request_meta_to_day(request.meta)

    res = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(
                name="attr_value",
                expression=f.distinct(column("attr_value", alias="attr_value")),
            ),
        ],
        condition=_build_base_conditions_and(request),
        order_by=[
            OrderBy(direction=OrderByDirection.ASC, expression=column("attr_value")),
        ],
        limit=request.limit,
        offset=request.page_token.offset,
    )
    treeify_or_and_conditions(res)
    return res


def _build_snuba_request(
    request: TraceItemAttributeValuesRequest,
) -> SnubaRequest:
    if not should_use_items_attrs(request.meta):
        return build_snuba_request_legacy(request)
    raise NotImplementedError


class AttributeValuesRequest(
    RPCEndpoint[TraceItemAttributeValuesRequest, TraceItemAttributeValuesResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TraceItemAttributeValuesRequest]:
        return TraceItemAttributeValuesRequest

    def _execute(
        self, in_msg: TraceItemAttributeValuesRequest
    ) -> TraceItemAttributeValuesResponse:
        snuba_request = _build_snuba_request(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        values = [r["attr_value"] for r in res.result.get("data", [])]
        if len(values) == 0:
            return TraceItemAttributeValuesResponse(
                values=values,
                page_token=None,
            )

        return TraceItemAttributeValuesResponse(
            values=values,
            page_token=(
                PageToken(offset=in_msg.page_token.offset + len(values))
                if in_msg.page_token.HasField("offset") or len(values) == 0
                else PageToken(
                    filter_offset=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                type=AttributeKey.TYPE_STRING, name="attr_value"
                            ),
                            op=ComparisonFilter.OP_GREATER_THAN,
                            value=AttributeValue(val_str=values[-1]),
                        )
                    )
                )
            ),
        )
