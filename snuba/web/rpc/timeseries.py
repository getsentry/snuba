import uuid
from datetime import datetime

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
from snuba.query.conditions import combine_and_conditions, combine_or_conditions
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    NestedColumn,
    and_cond,
    column,
    in_cond,
    literal,
    literals_array,
)
from snuba.query.expressions import Expression, FunctionCall
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query

_HARDCODED_MEASUREMENT_NAME = "eap.measurement"


def _get_measurement_field(
    request: AggregateBucketRequest,
) -> Expression:
    field = NestedColumn("attr_num")
    # HACK
    return field[request.metric_name]


def _treeify_or_and_conditions(query: Query) -> None:
    """
    look for expressions like or(a, b, c) and turn them into or(a, or(b, c))
                              and(a, b, c) and turn them into and(a, and(b, c))

    even though clickhouse sql supports arbitrary amount of arguments there are other parts of the
    codebase which assume `or` and `and` have two arguments

    Adding this post-process step is easier than changing the rest of the query pipeline

    Note: does not apply to the conditions of a from_clause subquery (the nested one)
        this is bc transform_expressions is not implemented for composite queries
    """

    def transform(exp: Expression) -> Expression:
        if not isinstance(exp, FunctionCall):
            return exp

        if exp.function_name == "and":
            return combine_and_conditions(exp.parameters)
        elif exp.function_name == "or":
            return combine_or_conditions(exp.parameters)
        else:
            return exp

    query.transform_expressions(transform)


def _get_aggregate_func(
    request: AggregateBucketRequest,
) -> Expression:
    FuncEnum = AggregateBucketRequest.Function
    measurement_field = _get_measurement_field(request)
    alias = "measurement"
    lookup = {
        FuncEnum.FUNCTION_SUM: f.sum(measurement_field, alias=alias),
        FuncEnum.FUNCTION_AVERAGE: f.avg(measurement_field, alias=alias),
        FuncEnum.FUNCTION_COUNT: f.count(measurement_field, alias=alias),
        # curried functions PITA, to do later
        FuncEnum.FUNCTION_P50: cf.quantile(0.5)(measurement_field, alias=alias),
        FuncEnum.FUNCTION_P95: cf.quantile(0.95)(measurement_field, alias=alias),
        FuncEnum.FUNCTION_P99: cf.quantile(0.99)(measurement_field, alias=alias),
    }
    res = lookup.get(request.aggregate, None)
    if res is None:
        NotImplementedError()
    return res  # type: ignore


def _build_condition(request: AggregateBucketRequest) -> Expression:
    project_ids = in_cond(
        column("project_id"),
        literals_array(
            alias=None,
            literals=[literal(pid) for pid in request.meta.project_ids],
        ),
    )

    return and_cond(
        project_ids,
        f.equals(column("organization_id"), request.meta.organization_id),
        # HACK: timestamp name
        f.less(
            column("start_timestamp"),
            f.toDateTime(
                datetime.utcfromtimestamp(request.end_timestamp.seconds).isoformat()
            ),
        ),
        f.greaterOrEquals(
            column("start_timestamp"),
            f.toDateTime(
                datetime.utcfromtimestamp(request.start_timestamp.seconds).isoformat()
            ),
        ),
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
        condition=_build_condition(request),
        granularity=request.granularity_secs,
        groupby=[column("time")],
    )
    _treeify_or_and_conditions(res)
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
