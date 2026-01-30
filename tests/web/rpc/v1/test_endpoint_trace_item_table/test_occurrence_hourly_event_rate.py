"""
Tests for querying EAP with OCCURRENCE item type to calculate hourly event rates.

This test validates the ability to:
1. Query OCCURRENCE items grouped by group_id (issue)
2. Calculate hourly event rates based on event counts and time windows
3. Take the 95th percentile of hourly_event_rates across all issues in a project

The hourly event rate formula:
- For issues older than a week: hourly_event_rate = past_week_event_count / WEEK_IN_HOURS
- For issues newer than a week: hourly_event_rate = past_week_event_count / hours_since_first_seen
"""

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.formula_pb2 import Literal
from sentry_protos.snuba.v1.request_common_pb2 import (
    RequestMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
    Function,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

# Constants for time calculations
WEEK_IN_HOURS = 168  # 7 days * 24 hours


# Base time for test data - 3 hours ago to ensure data is within query window
BASE_TIME = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(
    hours=3
)

START_TIMESTAMP = Timestamp(seconds=int((BASE_TIME - timedelta(days=14)).timestamp()))
END_TIMESTAMP = Timestamp(seconds=int((BASE_TIME + timedelta(hours=1)).timestamp()))


def _create_occurrence_items(
    group_id: int,
    first_seen: datetime,
    event_count: int,
    base_timestamp: datetime,
    project_id: int = 1,
) -> list[bytes]:
    """
    Create OCCURRENCE items for a specific group (issue).

    Args:
        group_id: The issue/group identifier
        first_seen: When the issue was first seen
        event_count: Number of events to create for this issue
        base_timestamp: Base timestamp for event creation
        project_id: Project ID for the occurrences

    Returns:
        List of serialized occurrence messages
    """
    messages = []
    for i in range(event_count):
        # Spread events across the time window
        event_time = base_timestamp - timedelta(minutes=i * 5)
        messages.append(
            gen_item_message(
                start_timestamp=event_time,
                type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
                attributes={
                    "group_id": AnyValue(int_value=group_id),
                    "first_seen": AnyValue(double_value=first_seen.timestamp()),
                    "first_seen_hours_ago": AnyValue(
                        double_value=(base_timestamp - first_seen).total_seconds() / 3600
                    ),
                    "sentry.group_id": AnyValue(int_value=group_id),
                },
                project_id=project_id,
                remove_default_attributes=True,
            )
        )
    return messages


@pytest.fixture(autouse=False)
def setup_occurrence_data(clickhouse_db: None, redis_db: None) -> dict[str, Any]:
    """
    Set up test data with OCCURRENCE items representing different issues.

    Creates issues with varying:
    - Ages (some older than a week, some newer)
    - Event counts (to get different hourly rates)
    """
    items_storage = get_storage(StorageKey("eap_items"))
    now = BASE_TIME

    # Define test issues with different characteristics
    # Format: (group_id, first_seen_days_ago, event_count)
    test_issues = [
        # Old issues (> 1 week old) - rate = event_count / WEEK_IN_HOURS
        (1001, 14, 168),  # 14 days old, 168 events -> 1.0 events/hour
        (1002, 10, 336),  # 10 days old, 336 events -> 2.0 events/hour
        (1003, 8, 84),  # 8 days old, 84 events -> 0.5 events/hour
        (1004, 21, 504),  # 21 days old, 504 events -> 3.0 events/hour
        (1005, 30, 840),  # 30 days old, 840 events -> 5.0 events/hour
        # New issues (< 1 week old) - rate = event_count / hours_since_first_seen
        (2001, 3, 72),  # 3 days (72 hours) old, 72 events -> 1.0 events/hour
        (2002, 1, 48),  # 1 day (24 hours) old, 48 events -> 2.0 events/hour
        (2003, 5, 60),  # 5 days (120 hours) old, 60 events -> 0.5 events/hour
        (2004, 2, 144),  # 2 days (48 hours) old, 144 events -> 3.0 events/hour
        (2005, 4, 480),  # 4 days (96 hours) old, 480 events -> 5.0 events/hour
    ]

    all_messages: list[bytes] = []
    expected_rates: dict[int, float] = {}
    one_week_ago = now - timedelta(days=7)

    for group_id, days_ago, event_count in test_issues:
        first_seen = now - timedelta(days=days_ago)

        # Calculate expected hourly rate based on the formula
        if first_seen < one_week_ago:
            # Old issue: rate = event_count / WEEK_IN_HOURS
            hourly_rate = event_count / WEEK_IN_HOURS
        else:
            # New issue: rate = event_count / hours_since_first_seen
            hours_since_first_seen = days_ago * 24
            hourly_rate = event_count / hours_since_first_seen

        expected_rates[group_id] = hourly_rate

        messages = _create_occurrence_items(
            group_id=group_id,
            first_seen=first_seen,
            event_count=event_count,
            base_timestamp=now,
        )
        all_messages.extend(messages)

    write_raw_unprocessed_events(items_storage, all_messages)  # type: ignore

    return {
        "expected_rates": expected_rates,
        "test_issues": test_issues,
        "now": now,
        "one_week_ago": one_week_ago,
    }


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestOccurrenceHourlyEventRate(BaseApiTest):
    """Tests for calculating hourly event rates from OCCURRENCE items."""

    def test_count_occurrences_by_group(self, setup_occurrence_data: dict[str, Any]) -> None:
        """
        Test that we can count occurrences grouped by group_id.

        This is the foundation for calculating hourly event rates.
        """
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.occurrence_hourly_rate",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id")),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"),
                        label="event_count",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_INT, name="group_id")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"))
                ),
            ],
            limit=100,
        )

        response = EndpointTraceItemTable().execute(message)

        # Verify we got results for our test groups
        assert len(response.column_values) == 2
        group_id_col = response.column_values[0]
        count_col = response.column_values[1]

        assert group_id_col.attribute_name == "group_id"
        assert count_col.attribute_name == "event_count"

        # We should have results for multiple groups
        assert len(group_id_col.results) > 0

    def test_hourly_rate_for_old_issues_with_division(
        self, setup_occurrence_data: dict[str, Any]
    ) -> None:
        """
        Test calculating hourly event rate for old issues using formula division.

        For issues older than a week:
        hourly_event_rate = event_count / WEEK_IN_HOURS
        """
        # Filter for old issues (group_id starting with 100x)
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.occurrence_hourly_rate",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"),
                    op=ComparisonFilter.OP_LESS_THAN,
                    value=AttributeValue(val_int=2000),
                )
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id")),
                # Count events per group
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"),
                        label="event_count",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
                # Calculate hourly rate = count / WEEK_IN_HOURS using formula
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_COUNT,
                                key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"),
                                label="count_for_rate",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            ),
                        ),
                        right=Column(
                            literal=Literal(val_double=float(WEEK_IN_HOURS)),
                        ),
                    ),
                    label="hourly_rate",
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_INT, name="group_id")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"))
                ),
            ],
            limit=100,
        )

        response = EndpointTraceItemTable().execute(message)

        # Verify we got the hourly_rate column
        assert len(response.column_values) == 3
        hourly_rate_col = None
        for col in response.column_values:
            if col.attribute_name == "hourly_rate":
                hourly_rate_col = col
                break

        assert hourly_rate_col is not None
        assert len(hourly_rate_col.results) > 0

        # Verify the calculated rates match expected values
        expected_rates = setup_occurrence_data["expected_rates"]
        group_id_col = response.column_values[0]

        for i, group_id_val in enumerate(group_id_col.results):
            group_id = group_id_val.val_int
            if group_id in expected_rates and group_id < 2000:
                actual_rate = hourly_rate_col.results[i].val_double
                expected_rate = expected_rates[group_id]
                assert abs(actual_rate - expected_rate) < 0.01, (
                    f"Group {group_id}: expected {expected_rate}, got {actual_rate}"
                )

    def test_p95_hourly_rate_using_precomputed_attribute(
        self, setup_occurrence_data: dict[str, Any]
    ) -> None:
        """
        Test taking P95 of hourly event rates when the rate is stored as an attribute.

        This simulates a scenario where the hourly_event_rate is pre-computed
        and stored with each occurrence, allowing us to calculate aggregate
        percentiles directly.
        """
        # First, create occurrences with pre-computed hourly_event_rate attribute
        items_storage = get_storage(StorageKey("eap_items"))
        now = BASE_TIME

        # Create occurrences with explicit hourly_event_rate values
        # These represent different issues with known rates
        rates_to_test = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]

        messages = []
        for i, rate in enumerate(rates_to_test):
            group_id = 9000 + i
            # Create multiple occurrences for each group with the same rate
            for j in range(10):
                messages.append(
                    gen_item_message(
                        start_timestamp=now - timedelta(minutes=j),
                        type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
                        attributes={
                            "group_id": AnyValue(int_value=group_id),
                            "hourly_event_rate": AnyValue(double_value=rate),
                            "sentry.group_id": AnyValue(int_value=group_id),
                        },
                        project_id=1,
                        remove_default_attributes=True,
                    )
                )

        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        # Query P95 of hourly_event_rate grouped by project
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.occurrence_hourly_rate_p95",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"),
                    op=ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
                    value=AttributeValue(val_int=9000),
                )
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_P95,
                        key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="hourly_event_rate"),
                        label="p95_hourly_rate",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            limit=10,
        )

        response = EndpointTraceItemTable().execute(message)

        # Verify we got the P95 result
        assert len(response.column_values) == 1
        p95_col = response.column_values[0]
        assert p95_col.attribute_name == "p95_hourly_rate"
        assert len(p95_col.results) == 1

        # P95 of [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0] should be around 4.75
        # (since we have 10 values, P95 is approximately the 9.5th value)
        p95_value = p95_col.results[0].val_double
        assert 4.0 <= p95_value <= 5.0, f"Expected P95 around 4.5-5.0, got {p95_value}"

    def test_conditional_hourly_rate_using_two_aggregations(
        self, setup_occurrence_data: dict[str, Any]
    ) -> None:
        """
        Test calculating conditional hourly rates using conditional aggregations.

        This approach uses two separate aggregations:
        1. One for old issues (first_seen_hours_ago >= 168)
        2. One for new issues (first_seen_hours_ago < 168)

        The conditional aggregation allows filtering within the aggregate function.
        """
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.occurrence_conditional_rate",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id")),
                # Count for old issues (first_seen >= 168 hours ago)
                Column(
                    conditional_aggregation=AttributeConditionalAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"),
                        label="old_issue_count",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                        filter=TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="first_seen_hours_ago",
                                ),
                                op=ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
                                value=AttributeValue(val_double=float(WEEK_IN_HOURS)),
                            )
                        ),
                    ),
                ),
                # Count for new issues (first_seen < 168 hours ago)
                Column(
                    conditional_aggregation=AttributeConditionalAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"),
                        label="new_issue_count",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                        filter=TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="first_seen_hours_ago",
                                ),
                                op=ComparisonFilter.OP_LESS_THAN,
                                value=AttributeValue(val_double=float(WEEK_IN_HOURS)),
                            )
                        ),
                    ),
                ),
                # Get hours since first seen for rate calculation
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_MAX,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="first_seen_hours_ago"
                        ),
                        label="hours_age",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_INT, name="group_id")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"))
                ),
            ],
            limit=100,
        )

        response = EndpointTraceItemTable().execute(message)

        # Verify we got results with conditional counts
        assert len(response.column_values) >= 3

        # Find the columns by name
        old_count_col = None
        new_count_col = None
        for col in response.column_values:
            if col.attribute_name == "old_issue_count":
                old_count_col = col
            elif col.attribute_name == "new_issue_count":
                new_count_col = col

        assert old_count_col is not None or new_count_col is not None

    def test_p95_of_grouped_hourly_rates(self, setup_occurrence_data: dict[str, Any]) -> None:
        """
        Test calculating P95 of hourly rates computed per group.

        This is a two-step approach:
        1. First calculate hourly rates per group using division formula
        2. Then take P95 across all groups

        Note: In practice, this would require a subquery or post-processing,
        but we can test the P95 aggregation on pre-computed rates.
        """
        # Create new test data with pre-computed rates for P95 calculation
        items_storage = get_storage(StorageKey("eap_items"))
        now = BASE_TIME

        # Create 20 issues with varying hourly rates for P95 calculation
        hourly_rates = [
            0.1,
            0.2,
            0.5,
            0.8,
            1.0,
            1.2,
            1.5,
            1.8,
            2.0,
            2.5,
            3.0,
            3.5,
            4.0,
            4.5,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
        ]

        messages = []
        for i, rate in enumerate(hourly_rates):
            group_id = 8000 + i
            messages.append(
                gen_item_message(
                    start_timestamp=now - timedelta(minutes=i),
                    type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
                    attributes={
                        "group_id": AnyValue(int_value=group_id),
                        "computed_hourly_rate": AnyValue(double_value=rate),
                    },
                    project_id=1,
                    remove_default_attributes=True,
                )
            )

        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        # Query P95 of the computed hourly rates
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.occurrence_p95_grouped_rates",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"),
                    op=ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
                    value=AttributeValue(val_int=8000),
                )
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_P95,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="computed_hourly_rate"
                        ),
                        label="p95_rate",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(type=AttributeKey.TYPE_INT, name="group_id"),
                        label="total_count",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            limit=10,
        )

        response = EndpointTraceItemTable().execute(message)

        # Verify P95 calculation
        assert len(response.column_values) == 2

        p95_col = None
        for col in response.column_values:
            if col.attribute_name == "p95_rate":
                p95_col = col
                break

        assert p95_col is not None
        assert len(p95_col.results) == 1

        # P95 of 20 values should be around the 19th value (9.0)
        p95_value = p95_col.results[0].val_double
        assert 8.0 <= p95_value <= 10.0, f"Expected P95 around 9.0, got {p95_value}"
