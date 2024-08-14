import uuid

from google.protobuf.json_format import MessageToDict

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.protobufs import AggregateBucket_pb2
from snuba.query import SelectedExpression
from snuba.query.conditions import combine_and_conditions, combine_or_conditions
from snuba.query.data_source.simple import Entity
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
    request: AggregateBucket_pb2.AggregateBucketRequest,
) -> Expression:
    field = NestedColumn("attr_num")
    # HACK
    return field[_HARDCODED_MEASUREMENT_NAME]


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
    request: AggregateBucket_pb2.AggregateBucketRequest,
) -> Expression:
    FuncEnum = AggregateBucket_pb2.AggregateBucketRequest.Function
    lookup = {
        FuncEnum.SUM: f.sum(_get_measurement_field(request)),
        FuncEnum.AVERAGE: f.avg(_get_measurement_field(request)),
        FuncEnum.COUNT: f.count(_get_measurement_field(request)),
        # curried functions PITA, to do later
        FuncEnum.P50: None,
        FuncEnum.P95: None,
        FuncEnum.P99: None,
        FuncEnum.AVG: None,
    }
    res = lookup[request.aggregate]
    if res is None:
        NotImplementedError()
    return res


def _build_condition(request: AggregateBucket_pb2.AggregateBucketRequest) -> Expression:
    project_ids = in_cond(
        column("project_id"),
        literals_array(
            alias=None,
            literals=[literal(pid) for pid in request.request_info.project_ids],
        ),
    )
    return and_cond(
        project_ids,
        f.equals(column("organization_id"), request.request_info.organization_id),
        # HACK: timestamp name
        f.less(
            column("start_timestamp"),
            f.fromUnixTimestamp(request.request_info.end_timestamp.seconds),
        ),
        f.greaterOrEquals(
            column("start_timestamp"),
            f.fromUnixTimestamp(request.request_info.start_timestamp.seconds),
        ),
    )


def _build_query(request: AggregateBucket_pb2.AggregateBucketRequest) -> Query:
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )

    res = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(name="agg", expression=_get_aggregate_func(request))
        ],
        condition=_build_condition(request),
        granularity=request.granularity_secs,
        groupby=[column("time")],
    )
    _treeify_or_and_conditions(res)
    return res


def _build_snuba_request(
    request: AggregateBucket_pb2.AggregateBucketRequest,
) -> SnubaRequest:

    return SnubaRequest(
        id=str(uuid.uuid4()),
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=HTTPQuerySettings(),
        attribution_info=AttributionInfo(
            referrer=request.request_info.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.request_info.organization_id,
                "referrer": request.request_info.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_timeseries",
        ),
    )


def timeseries_query(
    request: AggregateBucket_pb2.AggregateBucketRequest, timer: Timer | None = None
) -> AggregateBucket_pb2.AggregateBucketResponse:
    timer = timer or Timer("timeseries_query")
    snuba_request = _build_snuba_request(request)
    res = run_query(
        dataset=PluggableDataset(name="eap", all_entities=[]),
        request=snuba_request,
        timer=timer,
    )
    assert res.result.get("data", None) is not None
    return AggregateBucket_pb2.AggregateBucketResponse(
        result=[float(i) for i in res.result["data"]]
        # STUB
        # result=[float(i) for i in range(100)]
    )
