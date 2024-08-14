from snuba.protobufs import AggregateBucket_pb2
from snuba.utils.metrics.timer import Timer


def timeseries_query(request: AggregateBucket_pb2.AggregateBucketRequest, timer: Timer) -> AggregateBucket_pb2.AggregateBucketResponse:


    return AggregateBucket_pb2.AggregateBucketResponse(result=[float(i) for i in range(100)])

