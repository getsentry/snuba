import itertools
import time
import uuid
from typing import Any, Iterable

from google.protobuf.json_format import MessageToDict
from snuba.web.rpc import RPCEndpoint
from sentry_protos.snuba.v1alpha.endpoint_aggregate_bucket_pb2 import (
    AggregateBucketRequest as AggregateBucketRequestProto,
    AggregateBucketResponse,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import and_cond
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    project_id_and_org_conditions,
    timestamp_in_range_condition,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.v1alpha.timeseries import aggregate_functions

EIGHT_HOUR_GRANULARITY = 60 * 60 * 8
ONE_HOUR_GRANULARITY = 60 * 60


class UnmergedTimeseriesQuerierResult:
    def __init__(self, start_ts: int, end_ts: int, aggregate_results: list[Any]):
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.raw_aggregate_results = aggregate_results


class TimeseriesQuerierResult:
    def __init__(self, start_ts: int, end_ts: int, aggregate_results: list[float]):
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.aggregate_results = aggregate_results


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

    def aggregate_bucket_request(
        self, start_ts: int, end_ts: int, bucket_size_secs: int
    ) -> SnubaRequest:
        entity = Entity(
            key=EntityKey("eap_spans"),
            schema=get_entity(EntityKey("eap_spans")).get_data_model(),
            sample=None,
        )

        query = Query(
            from_clause=entity,
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
        settings = HTTPQuerySettings()
        # we don't want to cache the "last bucket", we'll never get cache hits on it
        if (end_ts - start_ts) % bucket_size_secs == 0:
            clickhouse_settings = {
                "use_query_cache": "true",
                "query_cache_ttl": 60 * 5,  # 5 minutes
            }
            # store things in the query cache long-term if they are >4 hours old
            if end_ts < time.time() - 4 * 60 * 60:
                clickhouse_settings["query_cache_ttl"] = (
                    90 * 24 * 60 * 60
                )  # store this query cache entry for 90 days
            settings.set_clickhouse_settings(clickhouse_settings)

        return SnubaRequest(
            id=str(uuid.uuid4()),
            original_body=self.original_body,
            query=query,
            query_settings=settings,
            attribution_info=AttributionInfo(
                referrer=self.referrer,
                team="eap",
                feature="eap",
                tenant_ids={
                    "organization_id": self.organization_id,
                    "referrer": self.referrer,
                },
                app_id=AppID("eap"),
                parent_api="eap_timeseries",
            ),
        )

    def execute(
        self, start_ts: int, end_ts: int, bucket_size_secs: int
    ) -> UnmergedTimeseriesQuerierResult:
        data = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=self.aggregate_bucket_request(start_ts, end_ts, bucket_size_secs),
            timer=self.timer,
        ).result["data"]

        return UnmergedTimeseriesQuerierResult(
            start_ts=start_ts,
            end_ts=end_ts,
            aggregate_results=list(
                data[0][f"agg{agg_idx}"] for agg_idx in range(len(self.aggregates))
            ),
        )

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
        unmerged_results: Iterable[UnmergedTimeseriesQuerierResult],
        request_granularity: int,
    ) -> Iterable[TimeseriesQuerierResult]:
        # if we fulfilled a "1 day of data" request with 6 x 4 hour blocks, we need to merge those 6 back into
        # one big bucket to send back to the UI
        number_of_results_to_merge = self.granularity_secs // request_granularity
        while True:
            chunk = list(itertools.islice(unmerged_results, number_of_results_to_merge))
            if not chunk:
                return None
            yield TimeseriesQuerierResult(
                start_ts=chunk[0].start_ts,
                end_ts=chunk[-1].end_ts,
                aggregate_results=[
                    agg.merge(x.raw_aggregate_results[agg_idx] for x in chunk)
                    for agg_idx, agg in enumerate(self.aggregates)
                ],
            )

    def run(self) -> AggregateBucketResponse:
        # if you request one day of data, we'd ideally like to split that up into 6 requests of 4 hours of data
        # so that if you refresh the page, we don't have to aggregate a bunch of data again.
        # the request granularity is the size of requests that are ultimately sent to clickhouse,
        # so a day of data could be requested as 6 * 4 hour requests, then those 6 requests would be re-merged
        # into one big response (if necessary)
        request_granularity = self.get_request_granularity()

        all_results: Iterable[UnmergedTimeseriesQuerierResult] = (
            self.execute(
                start_ts,
                min(start_ts + request_granularity, self.end_ts),
                request_granularity,
            )
            for start_ts in range(
                self.rounded_start_ts, self.end_ts, request_granularity
            )
        )

        merged_results = self.merge_results(all_results, request_granularity)

        # TODO: allow multiple aggregates once proto is done
        return AggregateBucketResponse(
            result=[float(r.aggregate_results[0]) for r in merged_results]
        )




class AggregateBucketRequest(RPCEndpoint[AggregateBucketRequestProto, AggregateBucketResponse]):

    @classmethod
    def version(cls) -> str:
        return "v1alpha"


    def _execute(self, in_msg: AggregateBucketRequestProto) -> AggregateBucketResponse:
        querier = TimeseriesQuerier(in_msg, self._timer)
        resp: AggregateBucketResponse = querier.run()
        return resp
