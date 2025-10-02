import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_filter_pb2 import ExistsFilter, TraceItemFilter
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_flex_time import (
    OutcomesFlexTimeRoutingStrategy,
)
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.routing_strategies.common import (
    OutcomeCategory,
    store_outcomes_data,
)
from tests.web.rpc.v1.test_utils import BASE_TIME, gen_item_message

_LOG_COUNT = 120
_ORG_ID = 1
_PROJECT_ID = 1


@dataclass
class LogOutcomeDataPoint:
    time: datetime
    num_outcomes: int
    num_logs: int


def _store_logs_and_outcomes(data_points: list[LogOutcomeDataPoint]) -> None:
    items_storage = get_storage(StorageKey("eap_items"))

    messages = []
    outcome_data = []
    for data_point in data_points:
        messages.extend(
            [
                gen_item_message(
                    start_timestamp=data_point.time,
                    item_id=random.randint(0, 2**128 - 1).to_bytes(16, byteorder="little"),
                    type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    attributes={
                        "color": AnyValue(
                            string_value=random.choice(
                                [
                                    "red",
                                    "green",
                                    "blue",
                                ]
                            )
                        ),
                        "location": AnyValue(
                            string_value=random.choice(
                                [
                                    "mobile",
                                    "frontend",
                                    "backend",
                                ]
                            )
                        ),
                        "sentry.timestamp_precise": AnyValue(
                            double_value=int(data_point.time.timestamp()) + random.random()
                        ),
                    },
                    project_id=_PROJECT_ID,
                    organization_id=_ORG_ID,
                )
                for _ in range(data_point.num_logs)
            ]
        )
        outcome_data.append((data_point.time, data_point.num_outcomes))
    write_raw_unprocessed_events(items_storage, messages)  # type: ignore

    store_outcomes_data(
        outcome_data, OutcomeCategory.LOG_ITEM, org_id=_ORG_ID, project_id=_PROJECT_ID
    )


@pytest.mark.eap
@pytest.mark.redis_db
class TestTraceItemTableFlexTime:
    def test_paginate_within_time_window(self, eap: Any) -> None:

        data_points = []
        for hour in range(25):
            data_points.append(
                LogOutcomeDataPoint(
                    time=BASE_TIME - timedelta(hours=hour),
                    num_outcomes=10_000_000,
                    num_logs=_LOG_COUNT,
                )
            )
        _store_logs_and_outcomes(data_points)

        num_hours_to_query = 4
        # we store
        strategy = OutcomesFlexTimeRoutingStrategy()
        # we tell the routing strategy that the most items we can query is 20_000_000
        # this means that if we query a four hour time range, it will get split in two
        strategy.set_config_value("max_items_to_query", 20_000_000)

        start_timestamp = Timestamp(
            seconds=int((BASE_TIME - timedelta(hours=num_hours_to_query)).timestamp())
        )
        end_timestamp = Timestamp(seconds=int(BASE_TIME.timestamp()))

        limit_per_query = 120

        # querying 4 hours of data, split into two windows,
        # each window queries has 240 datapoints, 120 points at a time
        # means that we will run the query  a total of 4 times

        times_queried = 0
        expected_times_queried = 4
        end_pagination = PageToken(end_pagination=True)
        page_token = PageToken(offset=0)
        result_size = 120
        while page_token != end_pagination:
            times_queried += 1
            message = TraceItemTableRequest(
                meta=RequestMeta(
                    project_ids=[1],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    downsampled_storage_config=DownsampledStorageConfig(
                        mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY_FLEXTIME
                    ),
                ),
                filter=TraceItemFilter(
                    exists_filter=ExistsFilter(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                    )
                ),
                columns=[Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location"))],
                limit=limit_per_query,
                page_token=page_token,
            )
            response = EndpointTraceItemTable().execute(message)
            assert isinstance(response, TraceItemTableResponse)
            result_size = len(response.column_values[0].results)
            page_token = response.page_token
            assert result_size == limit_per_query

        assert times_queried == expected_times_queried

    def test_paginate_first_page_empty(self, eap: Any) -> None:
        data_points = [
            LogOutcomeDataPoint(
                time=BASE_TIME - timedelta(hours=0), num_outcomes=10_000_000, num_logs=0
            ),
            LogOutcomeDataPoint(
                time=BASE_TIME - timedelta(hours=1), num_outcomes=10_000_000, num_logs=0
            ),
            LogOutcomeDataPoint(
                time=BASE_TIME - timedelta(hours=2), num_outcomes=10_000_000, num_logs=0
            ),
            LogOutcomeDataPoint(
                time=BASE_TIME - timedelta(hours=3), num_outcomes=10_000_000, num_logs=120
            ),
        ]

        _store_logs_and_outcomes(data_points)

        num_hours_to_query = 4
        # every hour we store 120 items 3 hours ago, and none before that, prentending it's 10 million every hour
        strategy = OutcomesFlexTimeRoutingStrategy()
        # we tell the routing strategy that the most items we can query is 20_000_000
        # this means that if we query a four hour time range, it will get split in two
        strategy.set_config_value("max_items_to_query", 20_000_000)

        start_timestamp = Timestamp(
            seconds=int((BASE_TIME - timedelta(hours=num_hours_to_query)).timestamp())
        )
        end_timestamp = Timestamp(seconds=int(BASE_TIME.timestamp()))

        limit_per_query = 120

        # querying 4 hours of data, split into two windows,
        # only the second window will have data of 120 items
        # so we will run the query 2 times

        times_queried = 0
        expected_times_queried = 2
        end_pagination = PageToken(end_pagination=True)
        page_token = PageToken(offset=0)
        while page_token != end_pagination:
            times_queried += 1
            message = TraceItemTableRequest(
                meta=RequestMeta(
                    project_ids=[1],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    downsampled_storage_config=DownsampledStorageConfig(
                        mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY_FLEXTIME
                    ),
                ),
                filter=TraceItemFilter(
                    exists_filter=ExistsFilter(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                    )
                ),
                columns=[Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location"))],
                limit=limit_per_query,
                page_token=page_token,
            )
            response = EndpointTraceItemTable().execute(message)
            assert isinstance(response, TraceItemTableResponse)
            page_token = response.page_token
            result_size = len(response.column_values[0].results) if response.column_values else 0
            if times_queried == 1:
                assert result_size == 0
            elif times_queried == 2:
                assert result_size == 120
            else:
                assert False

        assert times_queried == expected_times_queried

    def test_paginate_unique_item_id(self, eap: Any) -> None:
        data_points = []
        for hour in range(25):
            data_points.append(
                LogOutcomeDataPoint(
                    time=BASE_TIME - timedelta(hours=hour),
                    num_outcomes=10_000_000,
                    num_logs=_LOG_COUNT,
                )
            )
        _store_logs_and_outcomes(data_points)

        num_hours_to_query = 4
        # we store
        strategy = OutcomesFlexTimeRoutingStrategy()
        # we tell the routing strategy that the most items we can query is 20_000_000
        # this means that if we query a four hour time range, it will get split in two
        strategy.set_config_value("max_items_to_query", 20_000_000)

        start_timestamp = Timestamp(
            seconds=int((BASE_TIME - timedelta(hours=num_hours_to_query)).timestamp())
        )
        end_timestamp = Timestamp(seconds=int(BASE_TIME.timestamp()))

        limit_per_query = 120

        # querying 4 hours of data, split into two windows,
        # each window queries has 240 datapoints, 120 points at a time
        # means that we will run the query  a total of 4 times

        times_queried = 0
        expected_times_queried = 4
        end_pagination = PageToken(end_pagination=True)
        page_token = PageToken(offset=0)
        result_size = 120
        while page_token != end_pagination:
            times_queried += 1
            message = TraceItemTableRequest(
                meta=RequestMeta(
                    project_ids=[1],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    downsampled_storage_config=DownsampledStorageConfig(
                        mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY_FLEXTIME
                    ),
                ),
                filter=TraceItemFilter(
                    exists_filter=ExistsFilter(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                    )
                ),
                columns=[
                    Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")),
                    Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.timestamp")
                    ),
                    Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.timestamp_precise"
                        )
                    ),
                    Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")),
                ],
                order_by=[
                    TraceItemTableRequest.OrderBy(
                        column=Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.timestamp")
                        ),
                        descending=True,
                    ),
                    TraceItemTableRequest.OrderBy(
                        column=Column(
                            key=AttributeKey(
                                type=AttributeKey.TYPE_STRING, name="sentry.timestamp_precise"
                            )
                        ),
                        descending=True,
                    ),
                    TraceItemTableRequest.OrderBy(
                        column=Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                        ),
                    ),
                ],
                limit=limit_per_query,
                page_token=page_token,
            )
            response = EndpointTraceItemTable().execute(message)
            breakpoint()
            assert isinstance(response, TraceItemTableResponse)
            result_size = len(response.column_values[0].results)
            page_token = response.page_token
            assert result_size == limit_per_query

        assert times_queried == expected_times_queried

    def test_order_by_aggregations(self, eap: Any) -> None:
        raise NotImplementedError
