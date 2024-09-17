import uuid
from datetime import datetime

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    AttributeValuesRequest,
    AttributeValuesResponse,
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
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query
from snuba.web.rpc.common import base_conditions_and, treeify_or_and_conditions
from snuba.web.rpc.exceptions import BadSnubaRPCRequestException


def _build_query(request: AttributeValuesRequest) -> Query:
    if request.limit > 1000:
        raise BadSnubaRPCRequestException("Limit can be at most 1000")

    entity = Entity(
        key=EntityKey("spans_str_attrs"),
        schema=get_entity(EntityKey("spans_str_attrs")).get_data_model(),
        sample=None,
    )

    # this table stores timestamp as toStartOfDay(x) in UTC, so if you request 4PM - 8PM on a specific day, nada
    start_timestamp = datetime.utcfromtimestamp(request.meta.start_timestamp.seconds)
    end_timestamp = datetime.utcfromtimestamp(request.meta.end_timestamp.seconds)
    if start_timestamp.day == end_timestamp.day:
        start_timestamp = start_timestamp.replace(
            day=start_timestamp.day - 1, hour=0, minute=0, second=0, microsecond=0
        )
        end_timestamp = end_timestamp.replace(day=end_timestamp.day + 1)
        request.meta.start_timestamp.seconds = int(start_timestamp.timestamp())
        request.meta.end_timestamp.seconds = int(end_timestamp.timestamp())

    res = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(
                name="attr_value",
                expression=f.distinct(column("attr_value", alias="attr_value")),
            ),
        ],
        condition=base_conditions_and(
            request.meta,
            f.equals(column("attr_key"), literal(request.name)),
            # multiSearchAny has special treatment with ngram bloom filters
            # https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#functions-support
            f.multiSearchAny(
                column("attr_value"),
                literals_array(None, [literal(request.value_substring_match)]),
            ),
        ),
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC, expression=column("organization_id")
            ),
            OrderBy(direction=OrderByDirection.ASC, expression=column("attr_key")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("attr_value")),
        ],
        limit=request.limit,
        offset=request.offset,
    )
    treeify_or_and_conditions(res)
    return res


def _build_snuba_request(
    request: AttributeValuesRequest,
) -> SnubaRequest:
    return SnubaRequest(
        id=str(uuid.uuid4()),
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


def trace_item_attribute_values_query(
    request: AttributeValuesRequest, timer: Timer | None = None
) -> AttributeValuesResponse:
    timer = timer or Timer("trace_item_values")
    snuba_request = _build_snuba_request(request)
    res = run_query(
        dataset=PluggableDataset(name="eap", all_entities=[]),
        request=snuba_request,
        timer=timer,
    )
    return AttributeValuesResponse(
        values=[r["attr_value"] for r in res.result.get("data", [])]
    )
