import itertools
import time
from typing import Any, Iterable, MutableMapping, Optional, Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1alpha.endpoint_aggregate_bucket_pb2 import (
    AggregateBucketRequest as AggregateBucketRequestProto,
)
from sentry_protos.snuba.v1alpha.endpoint_aggregate_bucket_pb2 import (
    AggregateBucketResponse,
)

from snuba.query import SelectedExpression
from snuba.query.dsl import and_cond
from snuba.query.logical import Query
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.eap_execute import run_eap_query_async
from snuba.web.rpc.v1alpha.common import (
    project_id_and_org_conditions,
    timestamp_in_range_condition,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.v1alpha.timeseries import aggregate_functions

EIGHT_HOUR_GRANULARITY = 60 * 60 * 8
ONE_HOUR_GRANULARITY = 60 * 60


class TimeseriesQuerier:
    def __init__(self, request: AggregateBucketRequestProto, timer: Timer):
        self.start_ts = request.meta.start_timestamp.seconds
        self.end_ts = request.meta.end_timestamp.seconds
        self.rounded_start_ts = self.start_ts - (
            self.start_ts % request.granularity_secs
        )
        self.granularity_secs = request.granularity_secs
        self.timer = timer
        self.original_body = MessageToDict(request)
        self.aggregates = [aggregate_functions.get_aggregate_func(request)]
        self.base_conditions = and_cond(
            project_id_and_org_conditions(request.meta),
            trace_item_filters_to_expression(request.filter),
        )
        self.referrer = request.meta.referrer
        self.organization_id = request.meta.organization_id

    def create_clickhouse_query(
        self, start_ts: int, end_ts: int, bucket_size_secs: int
    ) -> Query:
        query = Query(
            from_clause=None,
            selected_columns=[
                SelectedExpression(
                    name=f"agg{i}", expression=self.aggregates[i].expression
                )
                for i in range(len(self.aggregates))
            ],
            condition=and_cond(
                self.base_conditions,
                timestamp_in_range_condition(start_ts, end_ts),
            ),
        )
        treeify_or_and_conditions(query)
        return query

    @staticmethod
    def get_clickhouse_settings(
        start_ts: int, is_full_bucket: bool, bucket_size_secs: int
    ) -> Optional[MutableMapping[str, Any]]:
        # we don't want to cache the "last bucket", we'll never get cache hits on it
        if not is_full_bucket:
            return None

        clickhouse_settings: MutableMapping[str, Any] = {
            "use_query_cache": "true",
            "query_cache_ttl": 60 * 5,  # 5 minutes
        }
        # store things in the query cache long-term if they are >4 hours old
        if start_ts + bucket_size_secs < time.time() - 4 * 60 * 60:
            clickhouse_settings["query_cache_ttl"] = (
                90 * 24 * 60 * 60
            )  # store this query cache entry for 90 days
        return clickhouse_settings

    def get_request_granularity(self) -> int:
        if (
            self.granularity_secs % EIGHT_HOUR_GRANULARITY == 0
            and self.granularity_secs != EIGHT_HOUR_GRANULARITY
        ):
            return EIGHT_HOUR_GRANULARITY
        if self.granularity_secs % ONE_HOUR_GRANULARITY == 0:
            return ONE_HOUR_GRANULARITY
        return self.granularity_secs

    def merge_results(
        self,
        unmerged_results: Iterable[list[Any]],
        request_granularity: int,
    ) -> Iterable[list[float]]:
        # if we fulfilled a "1 day of data" request with 6 x 4 hour blocks, we need to merge those 6 back into
        # one big bucket to send back to the UI
        number_of_results_to_merge = self.granularity_secs // request_granularity
        while True:
            chunk = list(itertools.islice(unmerged_results, number_of_results_to_merge))
            if not chunk:
                return None
            yield [
                agg.merge(x[agg_idx] for x in chunk)
                for agg_idx, agg in enumerate(self.aggregates)
            ]

    def run(self) -> AggregateBucketResponse:
        # if you request one day of data, we'd ideally like to split that up into 6 requests of 4 hours of data
        # so that if you refresh the page, we don't have to aggregate a bunch of data again.
        # the request granularity is the size of requests that are ultimately sent to clickhouse,
        # so a day of data could be requested as 6 * 4 hour requests, then those 6 requests would be re-merged
        # into one big response (if necessary)
        request_granularity = self.get_request_granularity()

        all_results: Iterable[list[Any]] = (
            [
                run_eap_query_async(
                    dataset="eap_spans",
                    query=self.create_clickhouse_query(
                        start_ts,
                        min(start_ts + request_granularity, self.end_ts),
                        request_granularity,
                    ),
                    clickhouse_settings=self.get_clickhouse_settings(
                        start_ts,
                        start_ts + request_granularity < self.end_ts,
                        request_granularity,
                    ),
                    referrer="eap.timeseries",
                    organization_id=self.organization_id,
                    parent_api="eap.timeseries",
                    timer=self.timer,
                    original_body=self.original_body,
                )
                .result(60)
                .result["data"][0][f"agg{agg_idx}"]
                for agg_idx in range(len(self.aggregates))
            ]
            for start_ts in range(
                self.rounded_start_ts, self.end_ts, request_granularity
            )
        )

        merged_results = self.merge_results(all_results, request_granularity)

        # TODO: allow multiple aggregates once proto is done
        return AggregateBucketResponse(result=[float(r[0]) for r in merged_results])


class AggregateBucketRequest(
    RPCEndpoint[AggregateBucketRequestProto, AggregateBucketResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1alpha"

    @classmethod
    def request_class(cls) -> Type[AggregateBucketRequestProto]:
        return AggregateBucketRequestProto

    @classmethod
    def response_class(cls) -> Type[AggregateBucketResponse]:
        return AggregateBucketResponse

    def _execute(self, in_msg: AggregateBucketRequestProto) -> AggregateBucketResponse:
        querier = TimeseriesQuerier(in_msg, self._timer)
        resp: AggregateBucketResponse = querier.run()
        return resp
