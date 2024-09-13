import uuid

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1alpha.endpoint_aggregate_bucket_pb2 import (
    AggregateBucketRequest,
    AggregateBucketResponse,
)
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query
from snuba.web.rpc.common import (
    NORMALIZED_COLUMNS,
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.exceptions import BadSnubaRPCRequestException


def _get_aggregate_func(
    request: AggregateBucketRequest,
) -> Expression:
    key_expr = attribute_key_to_expression(request.key)
    exists_condition: Expression = literal(True)
    if request.key.name not in NORMALIZED_COLUMNS:
        if request.key.type == AttributeKey.TYPE_STRING:
            exists_condition = f.mapContains(
                column("attr_str"), literal(request.key.name)
            )
        else:
            exists_condition = f.mapContains(
                column("attr_num"), literal(request.key.name)
            )
    sampling_weight_expr = column("sampling_weight_2")
    sign_expr = column("sign")
    sampling_weight_times_sign = f.multiply(sampling_weight_expr, sign_expr)

    if request.aggregate == AggregateBucketRequest.FUNCTION_SUM:
        return f.sum(f.multiply(key_expr, sampling_weight_times_sign), alias="sum")
    if request.aggregate == AggregateBucketRequest.FUNCTION_COUNT:
        return f.sumIf(sampling_weight_times_sign, exists_condition, alias="count")
    if request.aggregate == AggregateBucketRequest.FUNCTION_AVERAGE:
        return f.divide(
            f.sum(f.multiply(key_expr, sampling_weight_times_sign)),
            f.sumIf(sampling_weight_times_sign, exists_condition, alias="count"),
            alias="avg",
        )
    if request.aggregate == AggregateBucketRequest.FUNCTION_P50:
        return cf.quantileTDigestWeighted(0.5)(
            key_expr, sampling_weight_expr, alias="p50"
        )
    if request.aggregate == AggregateBucketRequest.FUNCTION_P95:
        return cf.quantileTDigestWeighted(0.95)(
            key_expr, sampling_weight_expr, alias="p90"
        )
    if request.aggregate == AggregateBucketRequest.FUNCTION_P99:
        return cf.quantileTDigestWeighted(0.99)(
            key_expr, sampling_weight_expr, alias="p95"
        )

    raise BadSnubaRPCRequestException(
        f"Aggregate {request.aggregate} had an unknown or unset type"
    )


def _build_query(request: AggregateBucketRequest) -> Query:
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )

    res = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(name="time", expression=column("time", alias="time")),
            SelectedExpression(name="agg", expression=_get_aggregate_func(request)),
        ],
        condition=base_conditions_and(
            request.meta,
            trace_item_filters_to_expression(request.filter),
        ),
        granularity=request.granularity_secs,
        groupby=[column("time")],
        order_by=[OrderBy(direction=OrderByDirection.ASC, expression=column("time"))],
    )
    treeify_or_and_conditions(res)
    return res


def _build_snuba_request(
    request: AggregateBucketRequest,
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
            parent_api="eap_timeseries",
        ),
    )


def timeseries_query(
    request: AggregateBucketRequest, timer: Timer | None = None
) -> AggregateBucketResponse:
    timer = timer or Timer("timeseries_query")
    snuba_request = _build_snuba_request(request)
    res = run_query(
        dataset=PluggableDataset(name="eap", all_entities=[]),
        request=snuba_request,
        timer=timer,
    )
    assert res.result.get("data", None) is not None
    return AggregateBucketResponse(result=[float(r["agg"]) for r in res.result["data"]])
