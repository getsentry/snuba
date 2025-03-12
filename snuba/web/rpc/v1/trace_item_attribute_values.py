import uuid
from datetime import datetime, timedelta
from typing import Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeValuesRequest,
    TraceItemAttributeValuesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Storage
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, literal, literals_array
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    convert_filter_offset,
    project_id_and_org_conditions,
    treeify_or_and_conditions,
    truncate_request_meta_to_day,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.legacy.attributes_common import should_use_items_attrs
from snuba.web.rpc.v1.legacy.trace_item_attribute_values import (
    build_snuba_request as build_snuba_request_legacy,
)


def next_monday(dt: datetime) -> datetime:
    return dt + timedelta(days=(7 - dt.weekday()) or 7)


def prev_monday(dt: datetime) -> datetime:
    return dt - timedelta(days=(dt.weekday() % 7))


def get_time_range_condition(meta: RequestMeta) -> Expression:
    # round the lower timestamp to the previous monday
    lower_ts = meta.start_timestamp.ToDatetime().replace(hour=0, minute=0, second=0)

    # round the upper timestamp to the next monday
    upper_ts = meta.end_timestamp.ToDatetime().replace(hour=0, minute=0, second=0)

    return and_cond(
        f.less(
            column("timestamp"),
            f.toStartOfWeek(f.addDays(f.toDateTime(upper_ts), 7)),
        ),
        f.greaterOrEquals(
            column("timestamp"),
            f.toStartOfWeek(f.toDateTime(lower_ts)),
        ),
    )


def _build_base_conditions_and(request: TraceItemAttributeValuesRequest) -> Expression:
    # TODO: week conversions
    conditions: list[Expression] = [
        f.equals(column("attr_key"), literal(request.key.name)),
        f.equals(column("attr_type"), literal("string")),
    ]
    if request.meta.trace_item_type:
        conditions.append(f.equals(column("item_type"), request.meta.trace_item_type))
    if request.value_substring_match:
        conditions.append(
            f.multiSearchAny(
                column("attr_value"),
                literals_array(None, [literal(request.value_substring_match)]),
            )
        )
    if request.page_token.HasField("filter_offset"):
        conditions.append(convert_filter_offset(request.page_token.filter_offset))

    return and_cond(
        and_cond(
            project_id_and_org_conditions(request.meta),
            # timestamp should be converted to start and end of week
            get_time_range_condition(request.meta),
        ),
        *conditions
    )

    # return base_conditions_and(
    #    request.meta,
    #    *conditions
    # )


def _build_query(request: TraceItemAttributeValuesRequest) -> Query:
    if request.limit > 1000:
        raise BadSnubaRPCRequestException("Limit can be at most 1000")

    storage_key = StorageKey("items_attrs")
    entity = Storage(
        key=storage_key,
        schema=get_storage(storage_key).get_schema().get_columns(),
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
            parent_api="trace_item_values",
        ),
    )


class AttributeValuesRequest(
    RPCEndpoint[TraceItemAttributeValuesRequest, TraceItemAttributeValuesResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TraceItemAttributeValuesRequest]:
        return TraceItemAttributeValuesRequest

    @classmethod
    def response_class(cls) -> Type[TraceItemAttributeValuesResponse]:
        return TraceItemAttributeValuesResponse

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
