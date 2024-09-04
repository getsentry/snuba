import uuid

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1alpha.endpoint_aggregate_bucket_pb2 import (
    AggregateBucketRequest,
    AggregateBucketResponse,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query
from snuba.web.rpc.common import (
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.exceptions import BadSnubaRPCRequestException


def _get_aggregate_func(
    request: AggregateBucketRequest,
) -> Expression:
    key_col = attribute_key_to_expression(request.key)
    if request.aggregate == AggregateBucketRequest.FUNCTION_SUM:
        return f.sum(key_col, alias="sum")
    if request.aggregate == AggregateBucketRequest.FUNCTION_AVERAGE:
        return f.avg(key_col, alias="avg")
    if request.aggregate == AggregateBucketRequest.FUNCTION_COUNT:
        return f.count(key_col, alias="count")
    if request.aggregate == AggregateBucketRequest.FUNCTION_P50:
        return cf.quantile(0.5)(key_col, alias="p50")
    if request.aggregate == AggregateBucketRequest.FUNCTION_P95:
        return cf.quantile(0.95)(key_col, alias="p90")
    if request.aggregate == AggregateBucketRequest.FUNCTION_P99:
        return cf.quantile(0.99)(key_col, alias="p95")

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
