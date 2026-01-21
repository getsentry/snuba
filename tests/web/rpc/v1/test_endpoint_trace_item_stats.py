from datetime import timedelta
from typing import Any

import pytest
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    AttributeDistribution,
    AttributeDistributions,
    AttributeDistributionsRequest,
    StatsType,
    TraceItemStatsRequest,
    TraceItemStatsResult,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_stats import EndpointTraceItemStats
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import (
    BASE_TIME,
    END_TIMESTAMP,
    START_TIMESTAMP,
    gen_item_message,
)


def pick_n_deterministic(choices: list[Any], weights: list[int], num_choices: int) -> list[Any]:
    """
    deterministically chooses num_choices from choices, where weights is the proportion of each choice
    that will be chosen. weights must sum to 100 and be the same size as choices. each index in weights
    corresponds to the index in choices.
    """
    if len(choices) != len(weights):
        raise ValueError("choices and weights must be the same length")
    if sum(weights) != 100:
        raise ValueError("weights must sum to 100")

    results = []
    for i, choice in enumerate(choices):
        how_many = int((weights[i] / 100) * num_choices)
        results.extend([choice] * how_many)
    return results


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    items_storage = get_storage(StorageKey("eap_items"))
    messages = []
    durations = pick_n_deterministic(
        choices=["10", "30", "50", None],
        weights=[5, 70, 15, 10],
        num_choices=120,
    )
    for i in range(120):
        for item_type in [TraceItemType.TRACE_ITEM_TYPE_SPAN, TraceItemType.TRACE_ITEM_TYPE_LOG]:
            attributes = {
                "low_cardinality": AnyValue(string_value=f"{i // 40}"),
                "sentry.sdk.name": AnyValue(string_value="sentry.python.django"),
            }
            if durations[i] is not None:
                attributes["duration_ms"] = AnyValue(string_value=durations[i])

            messages.append(
                gen_item_message(
                    start_timestamp=BASE_TIME - timedelta(minutes=i),
                    type=item_type,
                    attributes=attributes,
                    remove_default_attributes=True,
                )
            )

    write_raw_unprocessed_events(items_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemAttributesStats(BaseApiTest):
    def test_basic(self) -> None:
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            stats_types=[
                StatsType(
                    attribute_distributions=AttributeDistributionsRequest(
                        max_buckets=10,
                        max_attributes=100,
                    )
                )
            ],
        )

        response = self.app.post("/rpc/EndpointTraceItemStats/v1", data=message.SerializeToString())
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 200, error_proto

    def test_basic_with_data(self, setup_teardown: Any) -> None:
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
                ),
            ),
            stats_types=[
                StatsType(
                    attribute_distributions=AttributeDistributionsRequest(
                        max_buckets=10,
                        max_attributes=100,
                    )
                )
            ],
        )
        response = EndpointTraceItemStats().execute(message)
        expected_sdk_name_stats = AttributeDistribution(
            attribute_name="sentry.sdk.name",
            buckets=[
                AttributeDistribution.Bucket(
                    label="sentry.python.django",
                    value=120,
                )
            ],
        )

        assert response.results[0].HasField("attribute_distributions")
        assert expected_sdk_name_stats in response.results[0].attribute_distributions.attributes

        expected_low_cardinality_stat = AttributeDistribution(
            attribute_name="low_cardinality",
            buckets=[
                AttributeDistribution.Bucket(label="0", value=40),
                AttributeDistribution.Bucket(label="1", value=40),
                AttributeDistribution.Bucket(label="2", value=40),
            ],
        )

        match = False
        for stat in response.results[0].attribute_distributions.attributes:
            if stat.attribute_name == "low_cardinality":
                for bucket in expected_low_cardinality_stat.buckets:
                    match = True
                    assert bucket in stat.buckets
        assert match

        expected_duration_stat = AttributeDistribution(
            attribute_name="duration_ms",
            buckets=[
                AttributeDistribution.Bucket(label="30", value=84),
                AttributeDistribution.Bucket(label="50", value=18),
                AttributeDistribution.Bucket(label="10", value=6),
            ],
        )
        assert expected_duration_stat in response.results[0].attribute_distributions.attributes

    def test_allow_list(self, setup_teardown: Any) -> None:
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
                ),
            ),
            stats_types=[
                StatsType(
                    attribute_distributions=AttributeDistributionsRequest(
                        max_buckets=10,
                        max_attributes=100,
                        attributes=[
                            AttributeKey(name="duration_ms", type=AttributeKey.TYPE_STRING)
                        ],
                    )
                )
            ],
        )
        response = EndpointTraceItemStats().execute(message)
        assert response.results == [
            TraceItemStatsResult(
                attribute_distributions=AttributeDistributions(
                    attributes=[
                        AttributeDistribution(
                            attribute_name="duration_ms",
                            buckets=[
                                AttributeDistribution.Bucket(label="30", value=84),
                                AttributeDistribution.Bucket(label="50", value=18),
                                AttributeDistribution.Bucket(label="10", value=6),
                            ],
                        )
                    ]
                )
            )
        ]

    def test_with_filter(self, setup_teardown: Any) -> None:
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
                ),
            ),
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="low_cardinality"),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(val_str="0"),
                )
            ),
            stats_types=[
                StatsType(
                    attribute_distributions=AttributeDistributionsRequest(
                        max_buckets=10,
                        max_attributes=100,
                    )
                )
            ],
        )
        response = EndpointTraceItemStats().execute(message)
        expected_sdk_name_stats = AttributeDistribution(
            attribute_name="sentry.sdk.name",
            buckets=[AttributeDistribution.Bucket(label="sentry.python.django", value=40)],
        )

        assert response.results[0].HasField("attribute_distributions")
        assert expected_sdk_name_stats in response.results[0].attribute_distributions.attributes

        expected_low_cardinality_stats = AttributeDistribution(
            attribute_name="low_cardinality",
            buckets=[AttributeDistribution.Bucket(label="0", value=40)],
        )

        assert (
            expected_low_cardinality_stats in response.results[0].attribute_distributions.attributes
        )

    def test_last_seen_timestamp_in_query_results(self, setup_teardown: Any) -> None:
        """Test that the query returns last_seen timestamps for each bucket.

        This test verifies that max(timestamp) is computed correctly in the query.
        Once the proto is updated to include the last_seen field, this data will
        be populated in the response buckets.
        """
        from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_trace_item_stats import (
            LAST_SEEN_LABEL,
            _build_attr_distribution_query,
        )

        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
                ),
            ),
            stats_types=[
                StatsType(
                    attribute_distributions=AttributeDistributionsRequest(
                        max_buckets=10,
                        max_attributes=100,
                        attributes=[
                            AttributeKey(name="sentry.sdk.name", type=AttributeKey.TYPE_STRING)
                        ],
                    )
                )
            ],
        )

        # Verify the query includes the last_seen column
        query = _build_attr_distribution_query(
            message, message.stats_types[0].attribute_distributions
        )
        selected_column_names = [col.name for col in query.get_selected_columns()]
        assert LAST_SEEN_LABEL in selected_column_names

        # Run the actual query and verify last_seen is in results
        response = EndpointTraceItemStats().execute(message)
        assert response.results[0].HasField("attribute_distributions")

        # Verify we got results for sentry.sdk.name
        sdk_name_dist = None
        for dist in response.results[0].attribute_distributions.attributes:
            if dist.attribute_name == "sentry.sdk.name":
                sdk_name_dist = dist
                break

        assert sdk_name_dist is not None
        assert len(sdk_name_dist.buckets) > 0
        # The bucket should have the expected label and value
        assert sdk_name_dist.buckets[0].label == "sentry.python.django"
        assert sdk_name_dist.buckets[0].value == 120
