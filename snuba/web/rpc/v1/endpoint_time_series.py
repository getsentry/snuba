import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, Type

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    DataPoint,
    TimeSeries,
    TimeSeriesRequest,
    TimeSeriesResponse,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import column
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    aggregation_to_expression,
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)


def _convert_result_timeseries(
    request: TimeSeriesRequest, data: Iterable[Dict[str, Any]]
) -> Iterable[TimeSeries]:
    # to convert the results, I need to know which were the groupby columns and which ones
    # were aggregations
    aggregation_labels = set([agg.label for agg in request.aggregations])
    group_by_labels = set([attr.name for attr in request.group_by])

    result_timeseries = defaultdict(TimeSeries)
    # create a mapping with (all the group by attribute key,val pairs as strs, label name)

    for row in data:
        group_by_map = {}

        for col_name, col_value in row.items():
            if col_name in group_by_labels:
                group_by_map[col_name] = col_value

        group_by_key = "|".join(str(group_by_map.items()))

        for col_name in aggregation_labels:
            time_series = result_timeseries[(group_by_key, col_name)]
            time_series.label = col_name
            time_series.buckets.append(
                Timestamp(seconds=int(datetime.fromisoformat(row["time"]).timestamp()))
            )
            time_series.data_points.append(
                DataPoint(data=float(row[col_name]), data_present=True)
            )  # TODO: data not always present
            if not time_series.group_by_attributes and group_by_map:
                time_series.group_by_attributes.update(group_by_map)

    return result_timeseries.values()


def _build_query(request: TimeSeriesRequest) -> Query:
    # TODO: This is hardcoded still
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )

    aggregation_columns = [
        SelectedExpression(
            name=aggregation.label, expression=aggregation_to_expression(aggregation)
        )
        for aggregation in request.aggregations
    ]

    groupby_columns = [
        SelectedExpression(
            name=attr_key.name, expression=attribute_key_to_expression(attr_key)
        )
        for attr_key in request.group_by
    ]

    res = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(name="time", expression=column("time", alias="time")),
            *aggregation_columns,
            *groupby_columns,
        ],
        granularity=request.granularity_secs,
        condition=base_conditions_and(
            request.meta, trace_item_filters_to_expression(request.filter)
        ),
        groupby=[
            column("time"),
            *[attribute_key_to_expression(attr_key) for attr_key in request.group_by],
        ],
        order_by=[OrderBy(expression=column("time"), direction=OrderByDirection.ASC)],
    )
    treeify_or_and_conditions(res)
    return res


def _build_snuba_request(request: TimeSeriesRequest) -> SnubaRequest:
    query_settings = (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_span_samples",
        ),
    )


class EndpointTimeSeries(RPCEndpoint[TimeSeriesRequest, TimeSeriesResponse]):
    @classmethod
    def version(cls):
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TimeSeriesRequest]:
        return TimeSeriesRequest

    @classmethod
    def response_class(cls) -> Type[TimeSeriesResponse]:
        return TimeSeriesResponse

    def _execute(self, in_msg: TimeSeriesRequest) -> TimeSeriesResponse:
        # TODO: Move this to base
        in_msg.meta.request_id = getattr(in_msg.meta, "request_id", None) or str(
            uuid.uuid4()
        )
        snuba_request = _build_snuba_request(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [self._timer],
        )

        return TimeSeriesResponse(
            result_timeseries=list(
                _convert_result_timeseries(in_msg, res.result.get("data", {}))
            ),
            meta=response_meta,
        )
