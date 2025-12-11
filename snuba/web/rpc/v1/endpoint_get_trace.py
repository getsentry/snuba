import json
import random
import uuid
from datetime import datetime
from operator import attrgetter
from typing import Any, Dict, Iterable, NamedTuple, Optional, Type

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_get_trace_pb2 import (
    GetTraceRequest,
    GetTraceResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    Array,
    AttributeKey,
    AttributeValue,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    TraceItemFilter,
)

from snuba import state
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, equals, literal, or_cond
from snuba.query.expressions import FunctionCall
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.settings import (
    ENABLE_TRACE_PAGINATION_DEFAULT,
    ENDPOINT_GET_TRACE_PAGINATION_MAX_ITEMS,
)
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    attribute_key_to_expression,
    project_id_and_org_conditions,
    timestamp_in_range_condition,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

NORMALIZED_COLUMNS_TO_INCLUDE_EAP_ITEMS = [
    "organization_id",
    "project_id",
    "trace_id",
    "sampling_factor",
    "attributes_bool",
    "attributes_int",
]
APPLY_FINAL_ROLLOUT_PERCENTAGE_CONFIG_KEY = "EndpointGetTrace.apply_final_rollout_percentage"

TIMESTAMP_FIELD_BY_ITEM_TYPE: dict[TraceItemType.ValueType, str] = {
    TraceItemType.TRACE_ITEM_TYPE_SPAN: "sentry.start_timestamp_precise",
}


class EndpointGetTracePageToken:
    """
    Page token for the EndpointGetTrace RPC. Used for pagination when results are too large to fit in a single response.
    It uses Protobuf PageToken as the data layer to send over the http, but we should only use this class to interact with
    it internally.
    """

    # We kind of use the page token in a way it wasnt fully designed for.
    # And we rely on knowing the encoding algorithm used to encode it in order to decode it.
    # But since this class handles it all, this complexity is abstracted.
    def __init__(
        self,
        last_item_index: int,
        last_seen_timestamp_precise: float,
        last_seen_item_id: str,
    ):
        """
        last_item_index: the index of the last item in GetTraceRequest.items that was being processed
        last_seen_timestamp_precise: timestamp of the last seen item, used for filter sorting,
        last_seen_item_id: id of the last seen item, used for filter sorting,
        """
        self.last_item_index = last_item_index
        # this is the precise timestamp stored in the attributes of the item
        self.last_seen_timestamp_precise = last_seen_timestamp_precise
        # this is the non-precise timestamp that is stored in the timestamp column of the eap_items table
        self.last_seen_timestamp = int(last_seen_timestamp_precise)
        self.last_seen_item_id = int(last_seen_item_id, 16)

    @classmethod
    def from_protobuf(cls, page_token: PageToken) -> Optional["EndpointGetTracePageToken"]:
        if page_token == PageToken():
            return None
        filters = page_token.filter_offset.and_filter.filters
        if len(filters) != 3:
            raise ValueError("Invalid page token")
        if not (
            filters[0].comparison_filter.key.name == "last_item_index"
            and filters[0].comparison_filter.key.type == AttributeKey.Type.TYPE_INT
        ):
            raise ValueError("Invalid page token")
        last_item_index = filters[0].comparison_filter.value.val_int
        if not (
            filters[1].comparison_filter.key.name == "last_seen_timestamp_precise"
            and filters[1].comparison_filter.key.type == AttributeKey.Type.TYPE_DOUBLE
        ):
            raise ValueError("Invalid page token")
        last_seen_timestamp_precise = filters[1].comparison_filter.value.val_double
        if not (
            filters[2].comparison_filter.key.name == "last_seen_item_id"
            and filters[2].comparison_filter.key.type == AttributeKey.Type.TYPE_STRING
        ):
            raise ValueError("Invalid page token")
        last_seen_item_id = filters[2].comparison_filter.value.val_str
        return cls(last_item_index, last_seen_timestamp_precise, last_seen_item_id)

    def to_protobuf(self) -> PageToken:
        filters = TraceItemFilter(
            and_filter=AndFilter(
                filters=[
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="last_item_index", type=AttributeKey.Type.TYPE_INT
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_int=self.last_item_index),
                        )
                    ),
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="last_seen_timestamp_precise",
                                type=AttributeKey.Type.TYPE_DOUBLE,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_double=self.last_seen_timestamp_precise),
                        )
                    ),
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="last_seen_item_id", type=AttributeKey.Type.TYPE_STRING
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str=hex(self.last_seen_item_id)),
                        )
                    ),
                ]
            )
        )
        return PageToken(filter_offset=filters)


def _build_query(
    request: GetTraceRequest,
    item: GetTraceRequest.TraceItem,
    limit: int | None = None,
    page_token: EndpointGetTracePageToken | None = None,
) -> Query:
    selected_columns: list[SelectedExpression] = [
        SelectedExpression(
            name="id",
            expression=(
                attribute_key_to_expression(
                    AttributeKey(name="sentry.item_id", type=AttributeKey.Type.TYPE_STRING)
                )
            ),
        ),
        SelectedExpression(
            name="timestamp",
            expression=f.cast(
                (
                    attribute_key_to_expression(
                        AttributeKey(
                            name=TIMESTAMP_FIELD_BY_ITEM_TYPE.get(
                                item.item_type, "sentry.timestamp"
                            ),
                            type=AttributeKey.Type.TYPE_DOUBLE,
                        )
                    )
                ),
                "Float64",
                alias="item_timestamp",
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
                name="attributes_string",
                expression=FunctionCall(
                    ("attributes_string"),
                    "mapConcat",
                    tuple(column(f"attributes_string_{i}") for i in range(40)),
                ),
            ),
            SelectedExpression(
                name="attributes_float",
                expression=FunctionCall(
                    ("attributes_float"),
                    "mapConcat",
                    tuple(column(f"attributes_float_{i}") for i in range(40)),
                ),
            ),
            SelectedExpression(
                name="attributes_array",
                expression=FunctionCall(
                    "attributes_array",
                    "toJSONString",
                    (column("attributes_array"),),
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
    page_token_filter = (
        [
            or_cond(
                f.greater(
                    column("timestamp"),
                    literal(page_token.last_seen_timestamp),
                ),
                f.greater(
                    column("item_timestamp"),
                    literal(page_token.last_seen_timestamp_precise),
                ),
                and_cond(
                    f.equals(
                        column("timestamp"),
                        literal(page_token.last_seen_timestamp),
                    ),
                    f.equals(
                        column("item_timestamp"),
                        literal(page_token.last_seen_timestamp_precise),
                    ),
                    f.greater(
                        f.reinterpretAsUInt128(f.reverse(f.unhex(column("item_id")))),
                        literal(page_token.last_seen_item_id),
                    ),
                ),
            )
        ]
        if page_token is not None
        else []
    )
    old_order_by = [
        OrderBy(
            direction=OrderByDirection.ASC,
            expression=column("item_timestamp"),
        ),
    ]
    new_order_by = [
        OrderBy(
            direction=OrderByDirection.ASC,
            expression=column("timestamp"),
        ),
        OrderBy(
            direction=OrderByDirection.ASC,
            expression=column("item_timestamp"),
        ),
        OrderBy(
            direction=OrderByDirection.ASC,
            expression=column("item_id"),
        ),
    ]
    if state.get_int_config("enable_trace_pagination", ENABLE_TRACE_PAGINATION_DEFAULT):
        order_by = new_order_by
    else:
        order_by = old_order_by
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
            *(page_token_filter),
        ),
        order_by=order_by,
        limit=limit,
    )
    if random.random() < _get_apply_final_rollout_percentage():
        query.set_final(True)

    span = sentry_sdk.get_current_span()
    if span:
        span.set_data("is_final", query.get_final())

    treeify_or_and_conditions(query)

    return query


def _get_apply_final_rollout_percentage() -> float:
    return (
        state.get_float_config(
            APPLY_FINAL_ROLLOUT_PERCENTAGE_CONFIG_KEY,
            0.0,
        )
        or 0.0
    )


def _build_snuba_request(
    request: GetTraceRequest,
    item: GetTraceRequest.TraceItem,
    limit: int | None,
    page_token: EndpointGetTracePageToken | None,
) -> SnubaRequest:
    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=_build_query(request, item, limit, page_token),
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


def convert_to_attribute_value(value: Any) -> AttributeValue:
    if isinstance(value, bool):
        return AttributeValue(
            val_bool=value,
        )
    elif isinstance(value, int):
        return AttributeValue(
            val_int=value,
        )
    elif isinstance(value, float):
        return AttributeValue(
            val_double=value,
        )
    elif isinstance(value, str):
        return AttributeValue(
            val_str=value,
        )
    elif isinstance(value, list):
        return AttributeValue(
            val_array=Array(values=[convert_to_attribute_value(v) for v in value])
        )
    elif isinstance(value, datetime):
        return AttributeValue(
            val_double=value.timestamp(),
        )
    else:
        raise BadSnubaRPCRequestException(f"data type unknown: {type(value)}")


def _value_to_attribute(key: str, value: Any) -> tuple[AttributeKey, AttributeValue]:
    if isinstance(value, bool):
        return (
            AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_BOOLEAN,
            ),
            convert_to_attribute_value(value),
        )
    elif isinstance(value, int):
        return (
            AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_INT,
            ),
            convert_to_attribute_value(value),
        )
    elif isinstance(value, float):
        return (
            AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_DOUBLE,
            ),
            convert_to_attribute_value(value),
        )
    elif isinstance(value, str):
        return (
            AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_STRING,
            ),
            convert_to_attribute_value(value),
        )
    elif isinstance(value, list):
        return (
            AttributeKey(name=key, type=AttributeKey.Type.TYPE_ARRAY),
            convert_to_attribute_value(value),
        )
    elif isinstance(value, datetime):
        return (
            AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_DOUBLE,
            ),
            convert_to_attribute_value(value),
        )
    else:
        raise BadSnubaRPCRequestException(f"data type unknown: {type(value)}")


ProcessedResults = NamedTuple(
    "ProcessedResults",
    [
        ("items", list[GetTraceResponse.Item]),
        ("last_seen_timestamp_precise", float),
        ("last_seen_id", str),
    ],
)


def _transform_array_value(value: dict[str, str]) -> Any:
    for t, v in value.items():
        if t == "Int":
            return int(v)
        if t == "Double":
            return float(v)
        if t in {"String", "Bool"}:
            return v
    raise BadSnubaRPCRequestException(f"array value type unknown: {type(v)}")


def _process_arrays(raw: str) -> dict[str, list[Any]]:
    parsed = json.loads(raw) or {}
    arrays = {}
    for key, values in parsed.items():
        arrays[key] = [_transform_array_value(v) for v in values]
    return arrays


def _process_results(
    data: Iterable[Dict[str, Any]],
) -> ProcessedResults:
    """
    Used to process the results returned from clickhouse in a single pass.
    If you have more processing to do on the results, you can do it here and
    return the results as another entry in the ProcessedResults named tuple.
    """
    items: list[GetTraceResponse.Item] = []
    last_seen_timestamp_precise = 0.0
    last_seen_id = ""

    for row in data:
        id = row.pop("id")
        ts = row.pop("timestamp")
        arrays = row.pop("attributes_array", "{}") or "{}"
        # We want to merge these values after to overwrite potential floats
        # with the same name.
        booleans = row.pop("attributes_bool", {}) or {}
        integers = row.pop("attributes_int", {}) or {}
        last_seen_timestamp_precise = float(ts)
        last_seen_id = id

        timestamp = Timestamp()
        # truncate to microseconds since we store microsecond precision only
        # then transform to nanoseconds
        timestamp.FromNanoseconds(int(ts * 1e6) * 1000)

        attributes: dict[str, GetTraceResponse.Item.Attribute] = {}

        def add_attribute(key: str, value: Any) -> None:
            attribute_key, attribute_value = _value_to_attribute(key, value)
            attributes[key] = GetTraceResponse.Item.Attribute(
                key=attribute_key,
                value=attribute_value,
            )

        for row_key, row_value in row.items():
            if isinstance(row_value, dict):
                for column_key, column_value in row_value.items():
                    add_attribute(column_key, column_value)
            else:
                add_attribute(row_key, row_value)

        attributes_array = _process_arrays(arrays)
        for array_key, array_value in attributes_array.items():
            add_attribute(array_key, array_value)

        for bool_key, bool_value in booleans.items():
            add_attribute(bool_key, bool_value)

        for int_key, int_value in integers.items():
            add_attribute(int_key, int_value)

        item = GetTraceResponse.Item(
            id=id,
            timestamp=timestamp,
            attributes=sorted(
                attributes.values(),
                key=attrgetter("key.name"),
            ),
        )
        items.append(item)

    return ProcessedResults(
        items=items,
        last_seen_timestamp_precise=last_seen_timestamp_precise,
        last_seen_id=last_seen_id,
    )


def _get_pagination_limit(user_requested_limit: int) -> int | None:
    """
    If the user requested a limit, we use the minimum of the user requested limit and
    the ENDPOINT_GET_TRACE_PAGINATION_MAX_ITEMS snuba setting.

    If the user requested a limit of 0, we assume the user did not pass a limit, we
    use the default ENDPOINT_GET_TRACE_PAGINATION_MAX_ITEMS.
    """
    if ENDPOINT_GET_TRACE_PAGINATION_MAX_ITEMS <= 0:
        # no limit unless the user requests one
        if ENDPOINT_GET_TRACE_PAGINATION_MAX_ITEMS < 0:
            sentry_sdk.capture_message(
                f"Pagination max items is negative, no global limit will be applied: {ENDPOINT_GET_TRACE_PAGINATION_MAX_ITEMS}"
            )
        if user_requested_limit > 0:
            return user_requested_limit
        return None

    if user_requested_limit > 0:
        return min(user_requested_limit, ENDPOINT_GET_TRACE_PAGINATION_MAX_ITEMS)
    return ENDPOINT_GET_TRACE_PAGINATION_MAX_ITEMS


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
        self.metrics.increment(
            "eap_trace_request_without_limit", 1, tags={"referrer": in_msg.meta.referrer}
        )

        enable_pagination = state.get_int_config(
            "enable_trace_pagination", ENABLE_TRACE_PAGINATION_DEFAULT
        )
        if enable_pagination:
            limit = _get_pagination_limit(in_msg.limit)
            page_token = EndpointGetTracePageToken.from_protobuf(in_msg.page_token)
        else:
            limit = None
            page_token = None

        query_results = []
        item_groups = []
        if page_token is None:
            start = 0
        else:
            start = page_token.last_item_index
        for i, item in enumerate(in_msg.items[start:], start=start):
            item_group, query_result, last_seen_timestamp_precise, last_seen_id = (
                self._query_item_group(in_msg, item, limit, page_token)
            )
            page_token = None
            query_results.append(query_result)
            if len(item_group.items) == 0:
                continue

            item_groups.append(item_group)

            if limit is not None:
                limit -= len(item_group.items)

            if limit is not None and limit <= 0:
                # create a page token, we have reached the limit
                page_token = EndpointGetTracePageToken(i, last_seen_timestamp_precise, last_seen_id)
                break

        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            query_results,
            [self._timer] * len(query_results),
        )
        if not enable_pagination:
            serialized_page_token = None
        elif page_token is None:
            serialized_page_token = PageToken(end_pagination=True)
        else:
            serialized_page_token = page_token.to_protobuf()
        return GetTraceResponse(
            item_groups=item_groups,
            meta=response_meta,
            trace_id=in_msg.trace_id,
            page_token=serialized_page_token,
        )

    def _query_item_group(
        self,
        in_msg: GetTraceRequest,
        item: GetTraceRequest.TraceItem,
        limit: int | None,
        page_token: EndpointGetTracePageToken | None,
    ) -> tuple[GetTraceResponse.ItemGroup, Any, float, str]:
        results = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=_build_snuba_request(in_msg, item, limit, page_token),
            timer=self._timer,
        )
        processed_results = _process_results(
            results.result.get("data", []),
        )
        items = processed_results.items
        last_seen_timestamp_precise = processed_results.last_seen_timestamp_precise
        last_seen_id = processed_results.last_seen_id
        return (
            GetTraceResponse.ItemGroup(
                item_type=item.item_type,
                items=items,
            ),
            results,
            last_seen_timestamp_precise,
            last_seen_id,
        )
