import uuid
from datetime import datetime
from operator import attrgetter
from typing import Any, Dict, Iterable, Type

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_get_trace_pb2 import (
    GetTraceRequest,
    GetTraceResponse,
)
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
from snuba.query.expressions import FunctionCall
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
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
from snuba.web.rpc.v1.resolvers.R_eap_items.common.common import (
    attribute_key_to_expression,
)

NORMALIZED_COLUMNS_TO_INCLUDE_EAP_ITEMS = [
    "organization_id",
    "project_id",
    "trace_id",
    "sampling_factor",
]


def _build_query(request: GetTraceRequest, item: GetTraceRequest.TraceItem) -> Query:
    selected_columns: list[SelectedExpression] = [
        SelectedExpression(
            name="id",
            expression=(
                attribute_key_to_expression(
                    AttributeKey(
                        name="sentry.item_id", type=AttributeKey.Type.TYPE_STRING
                    )
                )
            ),
        ),
        SelectedExpression(
            name="timestamp",
            expression=f.cast(
                (
                    attribute_key_to_expression(
                        AttributeKey(
                            name="sentry.start_timestamp_precise",
                            type=AttributeKey.Type.TYPE_DOUBLE,
                        )
                    )
                ),
                "Float64",
                alias="timestamp",
            ),
        ),
    ]

    if len(item.attributes) > 0:
        for attribute_key in item.attributes:
            selected_columns.append(
                SelectedExpression(
                    name=attribute_key.name,
                    expression=(attribute_key_to_expression(attribute_key)),
                )
            )
    else:
        selected_columns += [
            SelectedExpression(
                name=("attributes_string"),
                expression=FunctionCall(
                    ("attributes_string"),
                    "mapConcat",
                    tuple(column(f"attributes_string_{i}") for i in range(40)),
                ),
            ),
            SelectedExpression(
                name=("attributes_float"),
                expression=FunctionCall(
                    ("attributes_float"),
                    "mapConcat",
                    tuple(column(f"attributes_float_{i}") for i in range(40)),
                ),
            ),
        ]
        selected_columns.extend(
            map(
                lambda col_name: SelectedExpression(
                    name=col_name,
                    expression=column(
                        col_name,
                        alias=f"selected_{col_name}",
                    ),
                ),
                (NORMALIZED_COLUMNS_TO_INCLUDE_EAP_ITEMS),
            )
        )

    entity = Entity(
        key=EntityKey("eap_items"),
        schema=get_entity(EntityKey("eap_items")).get_data_model(),
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
                column("item_type"),
                literal(item.item_type),
            ),
            equals(
                column("trace_id"),
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


def _build_snuba_request(
    request: GetTraceRequest,
    item: GetTraceRequest.TraceItem,
) -> SnubaRequest:
    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=_build_query(request, item),
        query_settings=(
            setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
        ),
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


def _value_to_attribute(key: str, value: Any) -> tuple[AttributeKey, AttributeValue]:
    if isinstance(value, int):
        return (
            AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_INT,
            ),
            AttributeValue(
                val_int=value,
            ),
        )
    elif isinstance(value, float):
        return (
            AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_DOUBLE,
            ),
            AttributeValue(
                val_double=value,
            ),
        )
    elif isinstance(value, str):
        return (
            AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_STRING,
            ),
            AttributeValue(
                val_str=value,
            ),
        )
    elif isinstance(value, datetime):
        return (
            AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_DOUBLE,
            ),
            AttributeValue(
                val_double=value.timestamp(),
            ),
        )
    else:
        raise BadSnubaRPCRequestException(f"data type unknown: {type(value)}")


def _convert_results(
    data: Iterable[Dict[str, Any]],
) -> list[GetTraceResponse.Item]:
    items: list[GetTraceResponse.Item] = []

    for row in data:
        id = row.pop("id")
        ts = row.pop("timestamp")

        timestamp = Timestamp()
        # truncate to microseconds since we store microsecond precision only
        # then transform to nanoseconds
        timestamp.FromNanoseconds(int(ts * 1e6) * 1000)

        attributes: list[GetTraceResponse.Item.Attribute] = []

        def add_attribute(key: str, value: Any) -> None:
            attribute_key, attribute_value = _value_to_attribute(key, value)
            attributes.append(
                GetTraceResponse.Item.Attribute(
                    key=attribute_key,
                    value=attribute_value,
                )
            )

        for key, value in row.items():
            if isinstance(value, dict):
                for k, v in value.items():
                    add_attribute(k, v)
            else:
                add_attribute(key, value)

        item = GetTraceResponse.Item(
            id=id,
            timestamp=timestamp,
            attributes=sorted(
                attributes,
                key=attrgetter("key.name"),
            ),
        )
        items.append(item)

    return items


class EndpointGetTrace(RPCEndpoint[GetTraceRequest, GetTraceResponse]):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[GetTraceRequest]:
        return GetTraceRequest

    @classmethod
    def response_class(cls) -> Type[GetTraceResponse]:
        return GetTraceResponse

    def _execute(self, in_msg: GetTraceRequest) -> GetTraceResponse:
        in_msg.meta.request_id = getattr(in_msg.meta, "request_id", None) or str(
            uuid.uuid4()
        )
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [],
            [self._timer],
        )
        item_groups = [self._query_item_group(in_msg, item) for item in in_msg.items]
        return GetTraceResponse(
            item_groups=item_groups,
            meta=response_meta,
            trace_id=in_msg.trace_id,
        )

    def _query_item_group(
        self,
        in_msg: GetTraceRequest,
        item: GetTraceRequest.TraceItem,
    ) -> GetTraceResponse.ItemGroup:
        results = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=_build_snuba_request(in_msg, item),
            timer=self._timer,
        )
        items = _convert_results(
            results.result.get("data", []),
        )
        return GetTraceResponse.ItemGroup(
            item_type=item.item_type,
            items=items,
        )
