import uuid
from typing import Any, Dict, Iterable

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_get_trace_pb2 import (
    GetTraceRequest,
    GetTraceResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, equals, literal
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    project_id_and_org_conditions,
    timestamp_in_range_condition,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers import ResolverGetTrace


def _build_query(request: GetTraceRequest) -> Query:
    selected_columns: list[SelectedExpression] = [
        SelectedExpression(
            name="id",
            expression=column("span_id", alias="id"),
        ),
        SelectedExpression(
            name="timestamp",
            expression=column(
                "start_timestamp",
                alias="timestamp",
            ),
        ),
    ]
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )
    query = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=and_cond(
            project_id_and_org_conditions(request.meta),
            timestamp_in_range_condition(
                request.meta.start_timestamp.seconds,
                request.meta.end_timestamp.seconds,
            ),
            equals(
                f.cast(
                    column("trace_id"),
                    "String",
                    alias="trace_id",
                ),
                literal(request.trace_id),
            ),
        ),
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=column("timestamp"),
            ),
        ],
    )

    treeify_or_and_conditions(query)

    return query


def _build_snuba_request(request: GetTraceRequest) -> SnubaRequest:
    query_settings = (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=_build_query(request),
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
            parent_api="eap_span_samples",
        ),
    )


def _convert_results(
    data: Iterable[Dict[str, Any]],
) -> list[GetTraceResponse.Item]:
    items: list[GetTraceResponse.Item] = []

    for row in data:
        attributes: list[GetTraceResponse.Item.Attribute] = []
        id = row.pop("id")
        timestamp = Timestamp()

        timestamp.FromDatetime(row.pop("timestamp"))

        for key, value in row.items():
            if isinstance(value, int):
                attribute_type = AttributeKey.Type.TYPE_INT
                attribute_value = AttributeValue(
                    val_int=value,
                )
            elif isinstance(value, float):
                attribute_type = AttributeKey.Type.TYPE_FLOAT
                attribute_value = AttributeValue(
                    val_float=value,
                )
            elif isinstance(value, str):
                attribute_type = AttributeKey.Type.TYPE_STRING
                attribute_value = AttributeValue(
                    val_str=value,
                )
            else:
                raise BadSnubaRPCRequestException(f"data type unknown: {type(value)}")

            attributes.append(
                GetTraceResponse.Item.Attribute(
                    key=AttributeKey(
                        name=key,
                        type=attribute_type,
                    ),
                    value=attribute_value,
                )
            )
        item = GetTraceResponse.Item(
            id=id,
            timestamp=timestamp,
            attributes=attributes,
        )
        items.append(item)

    return items


class ResolverGetTraceEAPSpans(ResolverGetTrace):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_SPAN

    def resolve(self, in_msg: GetTraceRequest) -> GetTraceResponse:
        results = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=_build_snuba_request(in_msg),
            timer=self._timer,
        )
        items = _convert_results(results.result.get("data", []))
        item_groups: list[GetTraceResponse.ItemGroup] = [
            GetTraceResponse.ItemGroup(
                item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                items=items,
            ),
        ]
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [],
            [self._timer],
        )
        return GetTraceResponse(
            item_groups=item_groups,
            meta=response_meta,
            trace_id=in_msg.trace_id,
        )
