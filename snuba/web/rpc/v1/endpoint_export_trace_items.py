import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, NamedTuple, Type, cast

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_items_pb2 import (
    ExportTraceItemsRequest,
    ExportTraceItemsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, ArrayValue, TraceItem

from snuba import state
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import FunctionCall
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    BUCKET_COUNT,
    attribute_key_to_expression,
    base_conditions_and,
    process_arrays,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import setup_trace_query_settings
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

_DEFAULT_PAGE_SIZE = 10_000

from typing import Optional

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    TraceItemFilter,
)


class ExportTraceItemsPageToken:
    def __init__(
        self,
        last_seen_project_id: int,
        last_seen_item_type: TraceItemType.ValueType,
        last_seen_timestamp: float,
        last_seen_trace_id: str,
        last_seen_item_id: str,
    ):
        self.last_seen_project_id = last_seen_project_id
        self.last_seen_item_type = last_seen_item_type
        self.last_seen_timestamp = last_seen_timestamp
        self.last_seen_trace_id = last_seen_trace_id
        self.last_seen_item_id = last_seen_item_id

    @classmethod
    def from_protobuf(cls, page_token: PageToken) -> Optional["ExportTraceItemsPageToken"]:
        if page_token == PageToken():
            return None
        filters = page_token.filter_offset.and_filter.filters
        if len(filters) != 5:
            raise ValueError("Invalid page token")

        if not (
            filters[0].comparison_filter.key.name == "last_seen_project_id"
            and filters[0].comparison_filter.key.type == AttributeKey.Type.TYPE_INT
        ):
            raise ValueError("Invalid project id")
        last_seen_project_id = filters[0].comparison_filter.value.val_int
        if not (
            filters[1].comparison_filter.key.name == "last_seen_item_type"
            and filters[1].comparison_filter.key.type == AttributeKey.Type.TYPE_INT
        ):
            raise ValueError("Invalid item type")
        last_seen_item_type = filters[1].comparison_filter.value.val_int

        if not (
            filters[2].comparison_filter.key.name == "last_seen_timestamp"
            and filters[2].comparison_filter.key.type == AttributeKey.Type.TYPE_DOUBLE
        ):
            raise ValueError("Invalid timestamp")
        last_seen_timestamp = filters[2].comparison_filter.value.val_double

        if not (
            filters[3].comparison_filter.key.name == "last_seen_trace_id"
            and filters[3].comparison_filter.key.type == AttributeKey.Type.TYPE_STRING
        ):
            raise ValueError("Invalid trace id")
        last_seen_trace_id = filters[3].comparison_filter.value.val_str

        if not (
            filters[4].comparison_filter.key.name == "last_seen_item_id"
            and filters[4].comparison_filter.key.type == AttributeKey.Type.TYPE_STRING
        ):
            raise ValueError("Invalid item id")
        last_seen_item_id = filters[4].comparison_filter.value.val_str

        return cls(
            last_seen_project_id,
            cast(TraceItemType.ValueType, last_seen_item_type),
            last_seen_timestamp,
            last_seen_trace_id,
            last_seen_item_id,
        )

    def to_protobuf(self) -> PageToken:
        filters = TraceItemFilter(
            and_filter=AndFilter(
                filters=[
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="last_seen_project_id", type=AttributeKey.Type.TYPE_INT
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_int=self.last_seen_project_id),
                        )
                    ),
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="last_seen_item_type", type=AttributeKey.Type.TYPE_INT
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_int=self.last_seen_item_type),
                        )
                    ),
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="last_seen_timestamp", type=AttributeKey.Type.TYPE_DOUBLE
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_double=self.last_seen_timestamp),
                        )
                    ),
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="last_seen_trace_id", type=AttributeKey.Type.TYPE_STRING
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str=self.last_seen_trace_id),
                        )
                    ),
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="last_seen_item_id", type=AttributeKey.Type.TYPE_STRING
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str=self.last_seen_item_id),
                        )
                    ),
                ]
            )
        )
        return PageToken(filter_offset=filters)


def _build_query(
    in_msg: ExportTraceItemsRequest, limit: int, page_token: ExportTraceItemsPageToken | None = None
) -> Query:
    selected_columns = [
        SelectedExpression("timestamp", f.toUnixTimestamp(column("timestamp"), alias="timestamp")),
        SelectedExpression(
            name="id",
            expression=(
                attribute_key_to_expression(
                    AttributeKey(name="sentry.item_id", type=AttributeKey.Type.TYPE_STRING)
                )
            ),
        ),
        SelectedExpression(
            "trace_id",
            column("trace_id", alias="trace_id"),
        ),
        SelectedExpression("organization_id", column("organization_id", alias="organization_id")),
        SelectedExpression("project_id", column("project_id", alias="project_id")),
        SelectedExpression("item_type", column("item_type", alias="item_type")),
        SelectedExpression(
            "client_sample_rate", column("client_sample_rate", alias="client_sample_rate")
        ),
        SelectedExpression(
            "server_sample_rate", column("server_sample_rate", alias="server_sample_rate")
        ),
        SelectedExpression("sampling_weight", column("sampling_weight", alias="sampling_weight")),
        SelectedExpression("sampling_factor", column("sampling_factor", alias="sampling_factor")),
        SelectedExpression(
            "attributes_string",
            f.mapConcat(
                *[column(f"attributes_string_{n}") for n in range(BUCKET_COUNT)],
                alias="attributes_string",
            ),
        ),
        SelectedExpression(
            "attributes_float",
            f.mapConcat(
                *[column(f"attributes_float_{n}") for n in range(BUCKET_COUNT)],
                alias="attributes_float",
            ),
        ),
        SelectedExpression("attributes_int", column("attributes_int", alias="attributes_int")),
        SelectedExpression("attributes_bool", column("attributes_bool", alias="attributes_bool")),
        SelectedExpression(
            "attributes_array",
            FunctionCall("attributes_array", "toJSONString", (column("attributes_array"),)),
        ),
    ]

    entity = Entity(
        key=EntityKey("eap_items"),
        schema=get_entity(EntityKey("eap_items")).get_data_model(),
        sample=None,
    )

    page_token_filter = (
        [
            f.greater(
                f.tuple(
                    column("project_id"),
                    column("item_type"),
                    column("timestamp"),
                    column("trace_id"),
                    column("item_id"),
                ),
                f.tuple(
                    literal(page_token.last_seen_project_id),
                    literal(page_token.last_seen_item_type),
                    literal(page_token.last_seen_timestamp),
                    literal(page_token.last_seen_trace_id),
                    literal(page_token.last_seen_item_id),
                ),
            )
        ]
        if page_token is not None
        else []
    )
    query = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(in_msg.meta, *(page_token_filter)),
        order_by=[
            # we add organization_id and project_id to the order by to optimize data reading
            # https://clickhouse.com/docs/sql-reference/statements/select/order-by#optimization-of-data-reading
            OrderBy(direction=OrderByDirection.ASC, expression=column("organization_id")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("project_id")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("item_type")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("timestamp")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("trace_id")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("item_id")),
        ],
        limit=limit,
    )

    treeify_or_and_conditions(query)
    return query


def _build_snuba_request(
    in_msg: ExportTraceItemsRequest, limit: int, page_token: ExportTraceItemsPageToken | None = None
) -> SnubaRequest:
    query_settings = setup_trace_query_settings() if in_msg.meta.debug else HTTPQuerySettings()
    query_settings.set_skip_transform_order_by(True)
    return SnubaRequest(
        id=uuid.UUID(in_msg.meta.request_id),
        original_body=MessageToDict(in_msg),
        query=_build_query(in_msg, limit, page_token),
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=in_msg.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": in_msg.meta.organization_id,
                "referrer": in_msg.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_gdpr_export",
        ),
    )


def _to_any_value(value: Any) -> AnyValue:
    if isinstance(value, bool):
        return AnyValue(bool_value=value)
    elif isinstance(value, int):
        return AnyValue(int_value=value)
    elif isinstance(value, float):
        return AnyValue(double_value=value)
    elif isinstance(value, str):
        return AnyValue(string_value=value)
    elif isinstance(value, list):
        return AnyValue(array_value=ArrayValue(values=[_to_any_value(v) for v in value]))
    elif isinstance(value, datetime):
        return AnyValue(double_value=value.timestamp())
    else:
        raise BadSnubaRPCRequestException(f"data type unknown: {type(value)}")


ProcessedResults = NamedTuple(
    "ProcessedResults",
    [
        ("items", list[TraceItem]),
        ("last_seen_project_id", int),
        ("last_seen_item_type", TraceItemType.ValueType),
        ("last_seen_timestamp", float),
        ("last_seen_trace_id", str),
        ("last_seen_item_id", str),
    ],
)


def _convert_rows(rows: Iterable[Dict[str, Any]]) -> ProcessedResults:
    items: list[TraceItem] = []
    last_seen_project_id = 0
    last_seen_item_type = TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED
    last_seen_timestamp = 0.0
    last_seen_trace_id = ""
    last_seen_item_id = ""

    for row in rows:
        org_id = row.pop("organization_id")
        proj_id = row.pop("project_id")
        trace_id = row.pop("trace_id")
        item_id = row.pop("id")
        item_type = row.pop("item_type")
        ts = row.pop("timestamp")
        client_sample_rate = row.pop("client_sample_rate", 1.0)
        server_sample_rate = row.pop("server_sample_rate", 1.0)
        sampling_factor = row.pop("sampling_factor", 1.0)  # noqa: F841
        sampling_weight = row.pop("sampling_weight", 1.0)  # noqa: F841
        arrays = row.pop("attributes_array", "{}") or "{}"
        booleans = row.pop("attributes_bool", {}) or {}
        integers = row.pop("attributes_int", {}) or {}
        floats = row.pop("attributes_float", {}) or {}

        attributes_map: dict[str, AnyValue] = {}

        for row_key, row_value in row.items():
            if isinstance(row_value, dict):
                for column_key, column_value in row_value.items():
                    attributes_map[column_key] = _to_any_value(column_value)
            else:
                attributes_map[row_key] = _to_any_value(row_value)

        attributes_array = process_arrays(arrays)
        for array_key, array_value in attributes_array.items():
            attributes_map[array_key] = _to_any_value(array_value)

        for bool_key, bool_value in booleans.items():
            attributes_map[bool_key] = _to_any_value(bool_value)

        for int_key, int_value in integers.items():
            attributes_map[int_key] = _to_any_value(int_value)

        for float_key, float_value in floats.items():
            attributes_map[float_key] = _to_any_value(float_value)

        timestamp = Timestamp()
        timestamp.FromNanoseconds(int(ts * 1e6) * 1000)

        item = TraceItem(
            organization_id=int(org_id),
            project_id=int(proj_id),
            trace_id=str(trace_id),
            item_id=bytes.fromhex(item_id),
            item_type=item_type,
            timestamp=timestamp,
            attributes=attributes_map,
            client_sample_rate=client_sample_rate,
            server_sample_rate=server_sample_rate,
        )
        items.append(item)

        last_seen_project_id = int(proj_id)
        last_seen_item_type = item_type
        last_seen_timestamp = float(ts)
        last_seen_trace_id = trace_id
        last_seen_item_id = item_id

    return ProcessedResults(
        items=items,
        last_seen_project_id=last_seen_project_id,
        last_seen_item_type=last_seen_item_type,
        last_seen_timestamp=last_seen_timestamp,
        last_seen_trace_id=last_seen_trace_id,
        last_seen_item_id=last_seen_item_id,
    )


class EndpointExportTraceItems(RPCEndpoint[ExportTraceItemsRequest, ExportTraceItemsResponse]):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[ExportTraceItemsRequest]:
        return ExportTraceItemsRequest

    @classmethod
    def response_class(cls) -> Type[ExportTraceItemsResponse]:
        return ExportTraceItemsResponse

    def _execute(self, in_msg: ExportTraceItemsRequest) -> ExportTraceItemsResponse:
        default_page_size = (
            state.get_int_config("export_trace_items_default_page_size", _DEFAULT_PAGE_SIZE)
            or _DEFAULT_PAGE_SIZE
        )
        if in_msg.limit > 0:
            limit = min(in_msg.limit, default_page_size)
        else:
            limit = default_page_size
        page_token = ExportTraceItemsPageToken.from_protobuf(in_msg.page_token)
        results = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=_build_snuba_request(in_msg, limit, page_token),
            timer=self._timer,
        )

        rows = results.result.get("data", [])
        processed_results = _convert_rows(rows)

        next_token: PageToken | None = None
        if len(processed_results.items) >= limit:
            next_token = ExportTraceItemsPageToken(
                last_seen_project_id=processed_results.last_seen_project_id,
                last_seen_item_type=processed_results.last_seen_item_type,
                last_seen_trace_id=processed_results.last_seen_trace_id,
                last_seen_timestamp=processed_results.last_seen_timestamp,
                last_seen_item_id=processed_results.last_seen_item_id,
            ).to_protobuf()
        else:
            next_token = PageToken(end_pagination=True)

        return ExportTraceItemsResponse(
            trace_items=processed_results.items,
            page_token=next_token,
        )
