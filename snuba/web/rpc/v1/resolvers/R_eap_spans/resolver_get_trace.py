import uuid
from datetime import datetime, timezone
from operator import attrgetter
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
from snuba.query.expressions import FunctionCall
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
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.common import (
    attribute_key_to_expression,
    attribute_key_to_expression_eap_items,
    use_eap_items_table,
)

_BUCKET_COUNT = 20


NORMALIZED_COLUMNS_TO_INCLUDE = [
    col.name
    for col in get_entity(EntityKey("eap_spans")).get_data_model().columns
    if col.name
    not in [
        "retention_days",
        "sign",
        "attr_str",
        "attr_num",
        "span_id",
        "timestamp",
        "time",
    ]
]

NORMALIZED_COLUMNS_TO_INCLUDE_EAP_ITEMS = [
    "organization_id",
    "project_id",
    "trace_id",
    "sampling_weight",
]

# Map of eap_items attributes to eap_spans attributes to preserve backwards compatibility
EAP_ITEMS_ATTRIBUTE_MAP = {
    "sentry.raw_description": "name",
    "sentry.transaction": "segment_name",
    "sentry.start_timestamp_precise": "start_timestamp",
    "sentry.end_timestamp_precise": "end_timestamp",
    "sentry.duration_ms": "duration_ms",
    "sentry.event_id": "event_id",
    "sentry.exclusive_time_ms": "exclusive_time_ms",
    "sentry.is_segment": "is_segment",
    "sentry.parent_span_id": "parent_span_id",
    "sentry.profile_id": "profile_id",
    "sentry.received": "received",
    "sentry.segment_id": "segment_id",
}

# Attributes that we store in eap_items but not eap_spans
ATTRIBUTES_TO_SKIP_FROM_EAP_ITEMS = [
    "event_id",
    "received",
]

# Attributes to hexify
HEX_COLUMNS = ["parent_span_id", "segment_id"]


def _build_query(request: GetTraceRequest) -> Query:
    selected_columns: list[SelectedExpression] = [
        SelectedExpression(
            name="id",
            expression=attribute_key_to_expression_eap_items(
                AttributeKey(name="sentry.span_id", type=AttributeKey.Type.TYPE_STRING)
            )
            if use_eap_items_table(request.meta)
            else column("span_id", alias="id"),
        ),
        SelectedExpression(
            name="timestamp",
            expression=f.cast(
                attribute_key_to_expression_eap_items(
                    AttributeKey(
                        name="sentry.start_timestamp_precise",
                        type=AttributeKey.Type.TYPE_DOUBLE,
                    )
                )
                if use_eap_items_table(request.meta)
                else column(
                    "start_timestamp",
                ),
                "Float64",
                alias="timestamp",
            ),
        ),
    ]
    item_conditions = [
        i for i in request.items if i.item_type == TraceItemType.TRACE_ITEM_TYPE_SPAN
    ][0]

    if len(item_conditions.attributes) > 0:
        for attribute_key in item_conditions.attributes:
            selected_columns.append(
                SelectedExpression(
                    name=attribute_key.name,
                    expression=attribute_key_to_expression_eap_items(attribute_key)
                    if use_eap_items_table(request.meta)
                    else attribute_key_to_expression(attribute_key),
                )
            )
    else:
        selected_columns += [
            SelectedExpression(
                name="attributes_string"
                if use_eap_items_table(request.meta)
                else "attrs_str",
                expression=FunctionCall(
                    "attributes_string"
                    if use_eap_items_table(request.meta)
                    else "attrs_str",
                    "mapConcat",
                    tuple(
                        column(
                            f"attributes_string_{i}"
                            if use_eap_items_table(request.meta)
                            else f"attr_str_{i}"
                        )
                        for i in range(
                            40 if use_eap_items_table(request.meta) else _BUCKET_COUNT
                        )
                    ),
                ),
            ),
            SelectedExpression(
                name="attributes_float"
                if use_eap_items_table(request.meta)
                else "attrs_num",
                expression=FunctionCall(
                    "attributes_float"
                    if use_eap_items_table(request.meta)
                    else "attrs_num",
                    "mapConcat",
                    tuple(
                        column(
                            f"attributes_float_{i}"
                            if use_eap_items_table(request.meta)
                            else f"attr_num_{i}"
                        )
                        for i in range(
                            40 if use_eap_items_table(request.meta) else _BUCKET_COUNT
                        )
                    ),
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
                NORMALIZED_COLUMNS_TO_INCLUDE_EAP_ITEMS
                if use_eap_items_table(request.meta)
                else NORMALIZED_COLUMNS_TO_INCLUDE,
            )
        )

        # special case for sampling_factor and service since we don't store them in eap_items
        if use_eap_items_table(request.meta):
            selected_columns.append(
                SelectedExpression(
                    name="sampling_factor",
                    expression=f.divide(
                        literal(1),
                        f.CAST(column("sampling_weight"), "Float64"),
                        alias="sampling_factor",
                    ),
                )
            )
            selected_columns.append(
                SelectedExpression(
                    name="service",
                    expression=f.CAST(column("project_id"), "String", alias="service"),
                )
            )
    if use_eap_items_table(request.meta):
        entity = Entity(
            key=EntityKey("eap_items"),
            schema=get_entity(EntityKey("eap_items")).get_data_model(),
            sample=None,
        )
    else:
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
    data: Iterable[Dict[str, Any]], add_hex_columns: bool = False
) -> list[GetTraceResponse.Item]:
    items: list[GetTraceResponse.Item] = []

    for row in data:
        id = row.pop("id")
        dt = datetime.fromtimestamp(row.pop("timestamp"), timezone.utc)

        timestamp = Timestamp()
        timestamp.FromDatetime(dt)

        attributes: list[GetTraceResponse.Item.Attribute] = []

        def add_attribute(key: str, value: Any) -> None:
            attribute_key, attribute_value = _value_to_attribute(key, value)
            attributes.append(
                GetTraceResponse.Item.Attribute(
                    key=attribute_key,
                    value=attribute_value,
                )
            )

        # marks which hex columns we've seen
        seen_hex_columns = set()
        for key, value in row.items():
            if isinstance(value, dict):
                for k, v in value.items():
                    k = EAP_ITEMS_ATTRIBUTE_MAP.get(k, k)
                    if k in ATTRIBUTES_TO_SKIP_FROM_EAP_ITEMS:
                        continue

                    if k in HEX_COLUMNS:
                        seen_hex_columns.add(k)
                    add_attribute(k, v)
            else:
                if key.lstrip("sentry.") in HEX_COLUMNS and value == "":
                    value = "0" * 16
                add_attribute(key, value)

        # this is a hack to get backwards compatibility to work, remove eventually
        if add_hex_columns:
            for k in HEX_COLUMNS:
                if k not in seen_hex_columns:
                    add_attribute(k, "0" * 16)

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
        item_conditions = [
            i for i in in_msg.items if i.item_type == TraceItemType.TRACE_ITEM_TYPE_SPAN
        ][0]
        items = _convert_results(
            results.result.get("data", []),
            not item_conditions.attributes,
        )
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
