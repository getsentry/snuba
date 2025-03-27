import uuid
from typing import Any, Dict, Iterable, Tuple, Type

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_details_pb2 import (
    TraceItemDetailsAttribute,
    TraceItemDetailsRequest,
    TraceItemDetailsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeValue

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    add_existence_check_to_subscriptable_references,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import (
    BadSnubaRPCRequestException,
    RPCRequestException,
)
from snuba.web.rpc.v1.resolvers.R_ourlogs.common.attribute_key_to_expression import (
    attribute_key_to_expression,
)

_BUCKET_COUNT = 40


def _build_query(request: TraceItemDetailsRequest) -> Query:
    entity = Entity(
        key=EntityKey("eap_items"),
        schema=get_entity(EntityKey("eap_items")).get_data_model(),
        sample=None,
    )

    res = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(
                "timestamp", f.toUnixTimestamp(column("timestamp"), alias="timestamp")
            ),
            SelectedExpression("hex_item_id", column("item_id", alias="hex_item_id")),
            SelectedExpression("trace_id", column("trace_id", alias="trace_id")),
            SelectedExpression(
                "organization_id", column("organization_id", alias="organization_id")
            ),
            SelectedExpression("project_id", column("project_id", alias="project_id")),
            SelectedExpression("item_type", column("item_type", alias="item_type")),
            SelectedExpression(
                "attributes_string",
                f.mapConcat(
                    *[column(f"attributes_string_{n}") for n in range(_BUCKET_COUNT)],
                    alias="attributes_string",
                ),
            ),
            SelectedExpression(
                "attributes_float",
                f.mapConcat(
                    *[column(f"attributes_float_{n}") for n in range(_BUCKET_COUNT)],
                    alias="attributes_float",
                ),
            ),
            SelectedExpression(
                "attributes_int", column("attributes_int", alias="attributes_int")
            ),
            SelectedExpression(
                "attributes_bool", column("attributes_bool", alias="attributes_bool")
            ),
        ],
        condition=base_conditions_and(
            request.meta,
            f.equals(column("item_type"), request.meta.trace_item_type),
            f.equals(
                column("item_id"),
                literal(request.item_id),
            ),
            trace_item_filters_to_expression(
                request.filter, attribute_key_to_expression
            ),
        ),
        limit=1,
    )
    treeify_or_and_conditions(res)
    add_existence_check_to_subscriptable_references(res)
    return res


def _build_snuba_request(request: TraceItemDetailsRequest) -> SnubaRequest:
    query_settings = (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )
    is_logs = request.meta.trace_item_type == TraceItemType.TRACE_ITEM_TYPE_LOG

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="ourlogs" if is_logs else "eap",
            feature="ourlogs" if is_logs else "eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="ourlog_trace_item_table" if is_logs else "eap_trace_item_table",
        ),
    )


def _convert_results(
    data: Iterable[Dict[str, Any]],
) -> Tuple[str, Timestamp, list[TraceItemDetailsAttribute]]:
    row = next(iter(data))
    item_id = row.pop("hex_item_id")
    dt = row.pop("timestamp")
    timestamp = Timestamp()
    timestamp.FromSeconds(dt)
    attrs = []

    if (val := row.pop("trace_id")) is not None:
        if val != "0" * 32:
            attrs.append(
                TraceItemDetailsAttribute(
                    name="sentry.trace_id",
                    value=AttributeValue(val_str=str(uuid.UUID(val))),
                )
            )
    if (val := row.pop("organization_id")) is not None:
        attrs.append(
            TraceItemDetailsAttribute(
                name="sentry.organization_id", value=AttributeValue(val_int=val)
            )
        )
    if (val := row.pop("project_id")) is not None:
        attrs.append(
            TraceItemDetailsAttribute(
                name="sentry.project_id", value=AttributeValue(val_int=val)
            )
        )
    if (val := row.pop("item_type")) is not None:
        attrs.append(
            TraceItemDetailsAttribute(
                name="sentry.item_type", value=AttributeValue(val_int=val)
            )
        )

    for k, v in row["attributes_string"].items():
        attrs.append(TraceItemDetailsAttribute(name=k, value=AttributeValue(val_str=v)))
    for k, v in row["attributes_float"].items():
        attrs.append(
            TraceItemDetailsAttribute(name=k, value=AttributeValue(val_float=v))
        )
    for k, v in row["attributes_int"].items():
        attrs.append(TraceItemDetailsAttribute(name=k, value=AttributeValue(val_int=v)))
    for k, v in row["attributes_bool"].items():
        attrs.append(
            TraceItemDetailsAttribute(name=k, value=AttributeValue(val_bool=v))
        )

    return item_id, timestamp, attrs


class EndpointTraceItemDetails(
    RPCEndpoint[TraceItemDetailsRequest, TraceItemDetailsResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TraceItemDetailsRequest]:
        return TraceItemDetailsRequest

    @classmethod
    def response_class(cls) -> Type[TraceItemDetailsResponse]:
        return TraceItemDetailsResponse

    def _execute(self, in_msg: TraceItemDetailsRequest) -> TraceItemDetailsResponse:
        if in_msg.meta.trace_item_type == TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
            raise BadSnubaRPCRequestException(
                "This endpoint requires meta.trace_item_type to be set (are you requesting spans? logs?)"
            )
        snuba_request = _build_snuba_request(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        try:
            item_id, timestamp, attributes = _convert_results(
                res.result.get("data", [])
            )
        except StopIteration:
            raise RPCRequestException(
                status_code=404,
                message=f"no item found with ID={in_msg.item_id}",
            )
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [self._timer],
        )
        return TraceItemDetailsResponse(
            meta=response_meta,
            item_id=item_id,
            timestamp=timestamp,
            attributes=attributes,
        )
