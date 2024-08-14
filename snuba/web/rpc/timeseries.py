import uuid

from google.protobuf.json_format import MessageToDict

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.protobufs import AggregateBucket_pb2
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer


def _build_query(request: AggregateBucket_pb2.AggregateBucketRequest) -> Query:
    raise NotImplementedError()


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
    request: AggregateBucket_pb2.AggregateBucketRequest, timer: Timer
) -> AggregateBucket_pb2.AggregateBucketResponse:

    return AggregateBucket_pb2.AggregateBucketResponse(
        result=[float(i) for i in range(100)]
    )
