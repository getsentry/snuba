import uuid

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeValuesRequest,
    TraceItemAttributeValuesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.protos.common import ATTRIBUTES_TO_COALESCE
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import combine_or_conditions
from snuba.query.data_source.simple import Entity, LogicalDataSource
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, map_key_exists
from snuba.query.expressions import Expression, FunctionCall
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    SEMVER_SORT_ATTRIBUTES,
    add_existence_check_to_subscriptable_references,
    attribute_key_to_expression,
    base_conditions_and,
    natural_sort_key,
    semver_sort_key,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
)


def _map_key_names_for_existence_check(request_key: AttributeKey) -> list[str]:
    """Map key names that may hold values (canonical plus deprecated/alias names)."""
    names = [request_key.name]
    for alt in ATTRIBUTES_TO_COALESCE.get(request_key.name, ()):
        if alt not in names:
            names.append(alt)
    return names


# Attribute value types this endpoint can enumerate, mapped to the ClickHouse
# attribute map they live in. The response proto only carries strings, so every
# supported type must also have a well-defined string form (see _execute).
_ATTRIBUTE_TYPE_TO_COLUMN: dict["AttributeKey.Type.ValueType", str] = {
    AttributeKey.TYPE_STRING: "attributes_string",
    AttributeKey.TYPE_BOOLEAN: "attributes_bool",
}


def _build_conditions(request: TraceItemAttributeValuesRequest) -> Expression:
    attribute_key = attribute_key_to_expression(request.key)

    try:
        attributes_column = _ATTRIBUTE_TYPE_TO_COLUMN[request.key.type]
    except KeyError as e:
        raise BadSnubaRPCRequestException("Only string and boolean attributes can be used") from e

    # Key existence via map_key_exists (has(mapKeys(col), key)); routed to the
    # right bucket for the bucketed string/float maps and the un-bucketed bool map.
    key_existence = combine_or_conditions(
        [
            map_key_exists(column(attributes_column), name)
            for name in _map_key_names_for_existence_check(request.key)
        ]
    )

    conditions: list[Expression] = [key_existence]
    if request.meta.trace_item_type:
        conditions.append(f.equals(column("item_type"), request.meta.trace_item_type))

    if request.value_substring_match:
        if request.key.type != AttributeKey.TYPE_STRING:
            raise BadSnubaRPCRequestException(
                "substring matches can only be used on string attributes"
            )
        conditions.append(
            f.like(
                attribute_key,
                f"%{request.value_substring_match}%",
            )
        )

    return base_conditions_and(request.meta, *conditions)


def _build_query(
    request: TraceItemAttributeValuesRequest,
) -> CompositeQuery[LogicalDataSource]:
    """Example query:


    SELECT distinct(attr_value) FROM
    (
        SELECT attributes_string_38['sentry.description'] as attr_value
        FROM eap_items_1_dist
        WHERE
        has(attributes_string_38, cityHash64('sentry.description'))
        AND attributes_string_38['sentry.description'] LIKE '%django.middleware%'
        AND project_id = 1 AND organization_id=1 AND item_type=1
        AND less(timestamp, toDateTime(1741910400))
        AND greaterOrEquals(timestamp, toDateTime(1741651200))
        ORDER BY attr_value
        LIMIT 10000
    ) ORDER BY attr_value LIMIT 1000


    This query will match the first 10000 occurrences of an attribute value and then deduplicate them,
    this gives a large speedup to the query at the cost of ordering and paginating all values
    """
    if request.limit > 10000:
        raise BadSnubaRPCRequestException("Limit can be at most 10000")

    entity_key = EntityKey("eap_items")
    entity = Entity(
        key=entity_key,
        schema=get_entity(entity_key).get_data_model(),
        sample=None,
    )
    attr_value = attribute_key_to_expression(request.key)
    assert attr_value.alias
    inner_query = Query(
        from_clause=entity,
        selected_columns=[SelectedExpression(name=attr_value.alias, expression=attr_value)],
        condition=_build_conditions(request),
        offset=0,
        limit=10000,
    )
    treeify_or_and_conditions(inner_query)
    add_existence_check_to_subscriptable_references(inner_query)
    # The value column normally orders lexicographically. When the caller opts
    # into SORT_NATURAL (sentry-protos#334), order it so embedded digit runs
    # compare numerically (e.g. "1.2.9" before "1.2.10"). Release attributes use
    # the release-aware semver key (prerelease before stable, "pkg@" prefix
    # stripped); every other attribute uses the general natural-sort key. count()
    # stays the primary ordering so the most common values still come first; the
    # sort key only changes the tiebreak among equally frequent values. An
    # unset/SORT_DEFAULT sort keeps the historical lexicographic order.
    if request.order_by.sort == TraceItemAttributeValuesRequest.OrderBy.SORT_NATURAL:
        if request.key.name in SEMVER_SORT_ATTRIBUTES:
            value_order_expression: Expression = semver_sort_key(column("attr_value"))
        else:
            value_order_expression = natural_sort_key(column("attr_value"))
    else:
        value_order_expression = column("attr_value")

    res = CompositeQuery(
        from_clause=inner_query,
        selected_columns=[
            SelectedExpression(
                name="attr_value",
                expression=column(attr_value.alias, alias="attr_value"),
            ),
            SelectedExpression(
                name="count()",
                expression=FunctionCall(
                    alias="count()",
                    function_name="count",
                    parameters=(),
                ),
            ),
        ],
        order_by=[
            OrderBy(direction=OrderByDirection.DESC, expression=column("count()")),
            OrderBy(direction=OrderByDirection.ASC, expression=value_order_expression),
        ],
        groupby=[column("attr_value")],
        limit=request.limit,
        offset=(request.page_token.offset if request.page_token.HasField("offset") else 0),
    )
    return res


def _build_snuba_request(
    request: TraceItemAttributeValuesRequest,
    routing_decision: RoutingDecision,
) -> SnubaRequest:
    settings = HTTPQuerySettings()
    settings.set_sampling_tier(routing_decision.tier)
    return SnubaRequest(
        id=uuid.uuid4(),
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=settings,
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
    def request_class(cls) -> type[TraceItemAttributeValuesRequest]:
        return TraceItemAttributeValuesRequest

    @classmethod
    def response_class(cls) -> type[TraceItemAttributeValuesResponse]:
        return TraceItemAttributeValuesResponse

    def _execute(self, in_msg: TraceItemAttributeValuesRequest) -> TraceItemAttributeValuesResponse:
        # if for some reason the item_id is the key, we can just return the value
        # item ids are unique
        if in_msg.key.name == "sentry.item_id" and in_msg.value_substring_match:
            return TraceItemAttributeValuesResponse(
                values=[in_msg.value_substring_match],
                counts=[1],
                page_token=None,
            )
        in_msg.limit = in_msg.limit or 1000
        snuba_request = _build_snuba_request(in_msg, self.routing_decision)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        # The response proto only carries strings. Boolean values are serialized
        # to the lowercase "true"/"false" form used across the EAP filter API so
        # that returned values round-trip as filter inputs.
        is_boolean = in_msg.key.type == AttributeKey.TYPE_BOOLEAN
        values, counts = [], []
        for row in res.result.get("data", []):
            value = row["attr_value"]
            values.append(str(bool(value)).lower() if is_boolean else value)
            counts.append(row.get("count()", 0))
        if len(values) == 0:
            return TraceItemAttributeValuesResponse(
                values=values,
                counts=counts,
                page_token=None,
            )
        return TraceItemAttributeValuesResponse(
            values=values,
            counts=counts,
            page_token=(
                PageToken(
                    offset=(in_msg.page_token.offset if in_msg.page_token.HasField("offset") else 0)
                    + len(values)
                )
            ),
        )
