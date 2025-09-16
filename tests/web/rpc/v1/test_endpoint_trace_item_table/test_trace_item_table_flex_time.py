import random
from datetime import timedelta
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_filter_pb2 import ExistsFilter, TraceItemFilter
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.routing_strategies.common import (
    OutcomeCategory,
    store_outcomes_data,
)
from tests.web.rpc.v1.test_utils import BASE_TIME, gen_item_message

_SPAN_COUNT = 120
_ORG_ID = 1
_PROJECT_ID = 1


@pytest.fixture(autouse=False)
def setup_teardown(eap: None, redis_db: None) -> None:
    items_storage = get_storage(StorageKey("eap_items"))
    # generate 120 items every hour
    messages = []
    for hour in range(25):
        messages.extend(
            [
                gen_item_message(
                    start_timestamp=BASE_TIME - timedelta(hours=i),
                    item_id=int("123456781234567d", 16).to_bytes(16, byteorder="little"),
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
                        "eap.measurement": AnyValue(
                            int_value=random.choice(
                                [
                                    1,
                                    100,
                                    1000,
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
                        "custom_measurement": AnyValue(double_value=420.0),
                        "custom_tag": AnyValue(string_value="blah"),
                    },
                    project_id=_PROJECT_ID,
                    organization_id=_ORG_ID,
                )
                for i in range(_SPAN_COUNT)
            ]
        )
    write_raw_unprocessed_events(items_storage, messages)  # type: ignore

    # pretend we have 10 million log items every hour
    outcome_data = []
    for hour in range(25):
        time = BASE_TIME - timedelta(hours=hour)
        outcome_data.append((time, 10_000_000))

    store_outcomes_data(
        outcome_data, OutcomeCategory.LOG_ITEM, org_id=_ORG_ID, project_id=_PROJECT_ID
    )


START_TIMESTAMP = Timestamp(seconds=int((BASE_TIME - timedelta(hours=24)).timestamp()))
END_TIMESTAMP = Timestamp(seconds=int(BASE_TIME.timestamp()))


@pytest.mark.eap
@pytest.mark.redis_db
class TestTraceItemTableFlexTime:
    def test_highest_accuracy_flextime_mode(self, eap: Any, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
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
            limit=10,
        )
        response = EndpointTraceItemTable().execute(message)
        assert isinstance(response, TraceItemTableResponse)
        breakpoint()
        print(response.page_token)
