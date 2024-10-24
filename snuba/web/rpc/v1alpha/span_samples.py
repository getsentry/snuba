import uuid
from typing import Any, Callable, Dict, Iterable, List, Sequence, Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1alpha.endpoint_span_samples_pb2 import SpanSample
from sentry_protos.snuba.v1alpha.endpoint_span_samples_pb2 import (
    SpanSamplesRequest as SpanSamplesRequestProto,
)
from sentry_protos.snuba.v1alpha.endpoint_span_samples_pb2 import SpanSamplesResponse
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import (
    AttributeKey,
    AttributeValue,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.v1alpha.common import (
    apply_virtual_columns,
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)


def _convert_order_by(
    order_by: Sequence[SpanSamplesRequestProto.OrderBy],
) -> Sequence[OrderBy]:
    res: List[OrderBy] = []
    for x in order_by:
        direction = OrderByDirection.DESC if x.descending else OrderByDirection.ASC
        res.append(
            OrderBy(
                direction=direction,
                expression=attribute_key_to_expression(x.key),
            )
        )
    return res


def _build_query(request: SpanSamplesRequestProto) -> Query:
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )

    selected_columns = []

    for key in request.keys:
        key_col = attribute_key_to_expression(key)
        selected_columns.append(SelectedExpression(name=key.name, expression=key_col))

    res = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            request.meta,
            trace_item_filters_to_expression(request.filter),
        ),
        order_by=_convert_order_by(request.order_by),
        limit=request.limit,
    )
    treeify_or_and_conditions(res)
    apply_virtual_columns(res, request.virtual_column_contexts)
    return res


def _build_snuba_request(
    request: SpanSamplesRequestProto,
) -> SnubaRequest:
    return SnubaRequest(
        id=uuid.uuid4(),
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=HTTPQuerySettings(),
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_span_samples",
        ),
    )


def _convert_results(
    request_keys: Sequence[AttributeKey], data: Iterable[Dict[str, Any]]
) -> Iterable[SpanSample]:
    converters: Dict[str, Callable[[Any], AttributeValue]] = {}

    for req_key in request_keys:
        if req_key.type == AttributeKey.TYPE_BOOLEAN:
            converters[req_key.name] = lambda x: AttributeValue(val_bool=bool(x))
        elif req_key.type == AttributeKey.TYPE_STRING:
            converters[req_key.name] = lambda x: AttributeValue(val_str=str(x))
        elif req_key.type == AttributeKey.TYPE_INT:
            converters[req_key.name] = lambda x: AttributeValue(val_int=int(x))
        elif req_key.type == AttributeKey.TYPE_FLOAT:
            converters[req_key.name] = lambda x: AttributeValue(val_float=float(x))

    for row in data:
        results = {}
        for attr_name, attr_val in row.items():
            results[attr_name] = converters[attr_name](attr_val)
        yield SpanSample(results=results)


class SpanSamplesRequest(RPCEndpoint[SpanSamplesRequestProto, SpanSamplesResponse]):
    @classmethod
    def version(cls) -> str:
        return "v1alpha"

    @classmethod
    def request_class(cls) -> Type[SpanSamplesRequestProto]:
        return SpanSamplesRequestProto

    @classmethod
    def response_class(cls) -> Type[SpanSamplesResponse]:
        return SpanSamplesResponse

    def _execute(self, in_msg: SpanSamplesRequestProto) -> SpanSamplesResponse:
        snuba_request = _build_snuba_request(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        span_samples = _convert_results(in_msg.keys, res.result.get("data", []))

        return SpanSamplesResponse(span_samples=span_samples)
