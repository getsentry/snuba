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
from sentry_relay.consts import DataCategory

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_flex_time import (
    OutcomesFlexTimeRoutingStrategy,
)
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.routing_strategies.common import store_outcomes_data
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
        for _ in range(data_point.num_logs):
            item_id = random.randint(0, 2**128 - 1).to_bytes(16, byteorder="big")
            message = gen_item_message(
                start_timestamp=data_point.time,
                item_id=item_id,
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
            messages.append(message)
        outcome_data.append((data_point.time, data_point.num_outcomes))
    write_raw_unprocessed_events(items_storage, messages)  # type: ignore

    store_outcomes_data(outcome_data, DataCategory.LOG_ITEM, org_id=_ORG_ID, project_id=_PROJECT_ID)


def get_item_ids_from_response(response: TraceItemTableResponse) -> list[str]:
    for column_value in response.column_values:
        if column_value.attribute_name == "sentry.item_id":
            return [item_id.val_str for item_id in column_value.results]
    return []


def _generate_table_request(
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
    page_token: PageToken | None = None,
    accuracy: DownsampledStorageConfig.Mode.ValueType = DownsampledStorageConfig.Mode.MODE_HIGHEST_ACCURACY_FLEXTIME,
    limit: int = 120,
    order_by: list[TraceItemTableRequest.OrderBy] | None = None,
    columns: list[Column] | None = None,
) -> TraceItemTableRequest:
    return TraceItemTableRequest(
        meta=RequestMeta(
            project_ids=[1],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
            downsampled_storage_config=DownsampledStorageConfig(mode=accuracy),
        ),
        filter=TraceItemFilter(
            exists_filter=ExistsFilter(
                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
            )
        ),
        columns=columns
        or [
            Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")),
            Column(key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="sentry.timestamp")),
            Column(
                key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="sentry.timestamp_precise")
            ),
            Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")),
        ],
        order_by=order_by
        or [
            TraceItemTableRequest.OrderBy(
                column=Column(
                    key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="sentry.timestamp")
                ),
                descending=True,
            ),
            TraceItemTableRequest.OrderBy(
                column=Column(
                    key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="sentry.timestamp_precise")
                ),
                descending=True,
            ),
            TraceItemTableRequest.OrderBy(
                column=Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                ),
                descending=True,
            ),
        ],
        limit=limit,
        page_token=page_token,
    )


@pytest.mark.eap
@pytest.mark.redis_db
class TestTraceItemTableFlexTime:
    def test_reject_request_without_order_by(self, eap: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.Mode.MODE_HIGHEST_ACCURACY_FLEXTIME
                ),
            ),
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_reject_request_without_order_by_timestamp_and_item_id(self, eap: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.Mode.MODE_HIGHEST_ACCURACY_FLEXTIME
                ),
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")),
                    descending=True,
                ),
            ],
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_reject_request_with_formula_column(self, eap: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.timestamp")),
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")),
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                        ),
                        right=Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                        ),
                    )
                ),
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
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                    ),
                    descending=True,
                ),
            ],
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_reject_request_with_bad_order_by_clause(self, eap: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.timestamp")
                    ),
                    descending=False,
                ),
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                    ),
                    descending=True,
                ),
            ],
        )

        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_paginate_within_time_window(self, eap: Any) -> None:
        num_hours_to_query = 4

        data_points = []
        for hour in range(num_hours_to_query + 1):
            data_points.append(
                LogOutcomeDataPoint(
                    time=BASE_TIME - timedelta(hours=hour),
                    num_outcomes=10_000_000,
                    num_logs=_LOG_COUNT,
                )
            )
        _store_logs_and_outcomes(data_points)

        start_timestamp = Timestamp(
            seconds=int((BASE_TIME - timedelta(hours=num_hours_to_query)).timestamp())
        )
        end_timestamp = Timestamp(seconds=int(BASE_TIME.timestamp()))

        all_ids_message = _generate_table_request(
            start_timestamp,
            end_timestamp,
            accuracy=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
            limit=3000,
        )
        response = EndpointTraceItemTable().execute(all_ids_message)
        stored_item_ids = get_item_ids_from_response(response)

        # we store
        strategy = OutcomesFlexTimeRoutingStrategy()
        # we tell the routing strategy that the most items we can query is 20_000_000
        # this means that if we query a four hour time range, it will get split in two
        strategy.set_config_value("max_items_to_query", 20_000_000)

        limit_per_query = 120

        # querying 4 hours of data, split into two windows,
        # each window queries has 240 datapoints, 120 points at a time
        # means that we will run the query  a total of 4 times

        times_queried = 0
        expected_times_queried = 4
        end_pagination = PageToken(end_pagination=True)
        page_token = PageToken(offset=0)
        result_size = 120

        queried_item_ids = []
        while page_token != end_pagination:
            times_queried += 1
            message = _generate_table_request(
                start_timestamp,
                end_timestamp,
                accuracy=DownsampledStorageConfig.Mode.MODE_HIGHEST_ACCURACY_FLEXTIME,
                limit=limit_per_query,
                page_token=page_token,
            )
            response = EndpointTraceItemTable().execute(message)
            assert isinstance(response, TraceItemTableResponse)
            result_size = len(response.column_values[0].results)
            page_token = response.page_token
            assert result_size == limit_per_query
            queried_item_ids.extend(get_item_ids_from_response(response))

        assert times_queried == expected_times_queried
        assert len(set(queried_item_ids)) == len(queried_item_ids)
        assert set(queried_item_ids) == set(stored_item_ids), set(stored_item_ids) - set(
            queried_item_ids
        )

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
            message = _generate_table_request(
                start_timestamp,
                end_timestamp,
                accuracy=DownsampledStorageConfig.Mode.MODE_HIGHEST_ACCURACY_FLEXTIME,
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

    def test_paginate_new_data_comes_in(self, eap: Any) -> None:
        data_points = []
        for hour in range(25):
            data_points.append(
                LogOutcomeDataPoint(
                    time=BASE_TIME
                    - timedelta(hours=hour)
                    + timedelta(minutes=random.randint(0, 59)),
                    num_outcomes=10_000_000,
                    num_logs=_LOG_COUNT,
                )
            )
        _store_logs_and_outcomes(data_points)
        num_hours_to_query = 4
        start_timestamp = Timestamp(
            seconds=int((BASE_TIME - timedelta(hours=num_hours_to_query)).timestamp())
        )
        end_timestamp = Timestamp(seconds=int(BASE_TIME.timestamp()))

        all_ids_message = _generate_table_request(
            start_timestamp,
            end_timestamp,
            accuracy=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY_FLEXTIME,
            limit=3000,
        )
        all_ids_response = EndpointTraceItemTable().execute(all_ids_message)
        all_ids_item_ids = get_item_ids_from_response(all_ids_response)

        # we store
        strategy = OutcomesFlexTimeRoutingStrategy()
        # we tell the routing strategy that the most items we can query is 20_000_000
        # this means that if we query a four hour time range, it will get split in two
        strategy.set_config_value("max_items_to_query", 20_000_000)

        limit_per_query = 120

        # querying 4 hours of data, split into two windows,
        # each window queries has 240 datapoints, 120 points at a time
        # means that we will run the query  a total of 4 times

        times_queried = 0
        end_pagination = PageToken(end_pagination=True)
        page_token = PageToken(offset=0)
        queried_item_ids: list[str] = []
        while page_token != end_pagination:
            times_queried += 1
            message = _generate_table_request(
                start_timestamp,
                end_timestamp,
                accuracy=DownsampledStorageConfig.Mode.MODE_HIGHEST_ACCURACY_FLEXTIME,
                limit=limit_per_query,
                page_token=page_token,
            )
            response = EndpointTraceItemTable().execute(message)
            assert isinstance(response, TraceItemTableResponse)
            # insert new data so we make sure the the page token lets us resume where we left off
            # event if new data comes in
            new_data_points = LogOutcomeDataPoint(
                time=BASE_TIME - timedelta(minutes=1),
                num_outcomes=1000,
                num_logs=1000,
            )
            queried_item_ids.extend(get_item_ids_from_response(response))
            _store_logs_and_outcomes([new_data_points])
            page_token = response.page_token

        # make sure there are no duplicates and we got all the items from the time range (before we added new data)
        assert len(set(queried_item_ids)) == len(queried_item_ids)
        # make sure all the data that was stored before the first query was retrieved (it's possible that there would be more retrieved
        # since we added more items as we queried)
        assert set(all_ids_item_ids).issubset(set(queried_item_ids))
