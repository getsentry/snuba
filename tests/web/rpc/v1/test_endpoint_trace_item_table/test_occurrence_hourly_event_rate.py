"""
Tests for querying EAP with OCCURRENCE item type to calculate hourly event rates.

Tests the per-group conditional hourly rate calculation:
  if(first_seen < one_week_ago): rate = count / WEEK_IN_HOURS
  else: rate = count / hours_since_first_seen
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

WEEK_IN_HOURS = 168  # 7 days * 24 hours


# Base time for test data - 3 hours ago to ensure data is within query window
BASE_TIME = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(
    hours=3
)

START_TIMESTAMP = Timestamp(seconds=int((BASE_TIME - timedelta(days=14)).timestamp()))
END_TIMESTAMP = Timestamp(seconds=int((BASE_TIME + timedelta(hours=1)).timestamp()))


def _count_agg(label: str = "") -> AttributeAggregation:
    return AttributeAggregation(
        aggregate=Function.FUNCTION_COUNT,
        key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id"),
        label=label,
        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
    )


def _min_ts_agg(label: str = "") -> AttributeAggregation:
    return AttributeAggregation(
        aggregate=Function.FUNCTION_MIN,
        key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="sentry.start_timestamp_precise"),
        label=label,
        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
    )


def _hours_since_first_seen(now_ts: float) -> Column:
    """(now - min(timestamp)) / 3600"""
    return Column(
        formula=Column.BinaryFormula(
            op=Column.BinaryFormula.OP_DIVIDE,
            left=Column(
                formula=Column.BinaryFormula(
                    op=Column.BinaryFormula.OP_SUBTRACT,
                    left=Column(literal=Literal(val_double=now_ts)),
                    right=Column(aggregation=_min_ts_agg()),
                ),
            ),
            right=Column(literal=Literal(val_double=3600.0)),
        ),
    )


def _conditional_rate_formula(
    one_week_ago_ts: float, now_ts: float, count_col: Column
) -> Column.ConditionalFormula:
    """if(min(ts) < one_week_ago, count/WEEK_IN_HOURS, count/hours_since_first_seen)"""
    FormulaCondition = Column.FormulaCondition
    return Column.ConditionalFormula(
        condition=FormulaCondition(
            left=Column(aggregation=_min_ts_agg()),
            op=FormulaCondition.OP_LESS_THAN,
            right=Column(literal=Literal(val_double=one_week_ago_ts)),
        ),
        **{
            "match": Column(
                formula=Column.BinaryFormula(
                    op=Column.BinaryFormula.OP_DIVIDE,
                    left=count_col,
                    right=Column(literal=Literal(val_double=float(WEEK_IN_HOURS))),
                ),
            ),
        },
        default=Column(
            formula=Column.BinaryFormula(
                op=Column.BinaryFormula.OP_DIVIDE,
                left=count_col,
                right=_hours_since_first_seen(now_ts),
                default_value_double=0.0,
            ),
        ),
    )


def _create_occurrence_items_for_group(
    group_id: int,
    timestamps: list[datetime],
    project_id: int = 1,
) -> list[bytes]:
    """
    Create OCCURRENCE items for a specific group (issue) at given timestamps.

    This simulates real occurrence data where each occurrence has a timestamp,
    and the first_seen is derived from min(timestamp) during query time.
    """
    messages = []
    for ts in timestamps:
        messages.append(
            gen_item_message(
                start_timestamp=ts,
                type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
                attributes={
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

    Creates issues with:
    - Varying first_seen times (some older than a week, some newer)
    - Events spread over the past week for counting
    """
    items_storage = get_storage(StorageKey("eap_items"))
    now = BASE_TIME
    one_week_ago = now - timedelta(days=7)

    # Test issues configuration:
    # (group_id, first_seen, events_in_past_week, expected_hourly_rate)
    #
    # For old issues (first_seen < one_week_ago):
    #   hourly_rate = events_in_past_week / WEEK_IN_HOURS
    #
    # For new issues (first_seen >= one_week_ago):
    #   hourly_rate = events_in_past_week / hours_since_first_seen

    test_issues: list[dict[str, Any]] = [
        # Old issues (first_seen > 1 week ago)
        {
            "group_id": 1001,
            "first_seen": now - timedelta(days=14),
            "events_in_past_week": 168,  # -> 1.0 events/hour
        },
        {
            "group_id": 1002,
            "first_seen": now - timedelta(days=10),
            "events_in_past_week": 336,  # -> 2.0 events/hour
        },
        {
            "group_id": 1003,
            "first_seen": now - timedelta(days=21),
            "events_in_past_week": 84,  # -> 0.5 events/hour
        },
        # New issues (first_seen < 1 week ago)
        {
            "group_id": 2001,
            "first_seen": now - timedelta(days=3),  # 72 hours ago
            "events_in_past_week": 72,  # -> 1.0 events/hour
        },
        {
            "group_id": 2002,
            "first_seen": now - timedelta(days=1),  # 24 hours ago
            "events_in_past_week": 48,  # -> 2.0 events/hour
        },
        {
            "group_id": 2003,
            "first_seen": now - timedelta(days=5),  # 120 hours ago
            "events_in_past_week": 60,  # -> 0.5 events/hour
        },
    ]

    all_messages: list[bytes] = []
    expected_rates: dict[int, float] = {}

    for issue in test_issues:
        group_id = issue["group_id"]
        first_seen = issue["first_seen"]
        event_count = issue["events_in_past_week"]

        # Calculate expected hourly rate
        if first_seen < one_week_ago:
            hourly_rate = event_count / WEEK_IN_HOURS
        else:
            hours_since_first_seen = (now - first_seen).total_seconds() / 3600
            hourly_rate = event_count / hours_since_first_seen

        expected_rates[group_id] = hourly_rate

        # Create timestamps for events:
        # - One at first_seen (to establish min timestamp)
        # - Rest spread evenly in the countable window (past week or since first_seen)
        spread_start = max(first_seen, one_week_ago)
        hours_available = max((now - spread_start).total_seconds() / 3600, 1)
        timestamps = [first_seen]
        for i in range(event_count - 1):
            offset_hours = (i + 1) * hours_available / event_count
            timestamps.append(spread_start + timedelta(hours=offset_hours))

        messages = _create_occurrence_items_for_group(group_id, timestamps[:event_count])
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
class TestOccurrenceHourlyEventRateSupported(BaseApiTest):
    """
    Tests for supported OCCURRENCE hourly event rate query patterns.

    These tests demonstrate the full hourly event rate calculation:
    - Basic aggregation per group (count, min timestamp)
    - Rate calculation with static and dynamic divisors
    - Conditional rate based on aggregate first_seen (via ConditionalFormula)
    - countIf combined with ConditionalFormula for filtered conditional rates
    - P95 of pre-computed attributes
    - End-to-end p95 of computed rates via two-step query approach
    """

    def test_count_and_min_timestamp_per_group(self, setup_occurrence_data: dict[str, Any]) -> None:
        """
        Test that we can:
        1. Group by sentry.group_id
        2. Count events per group
        3. Find first_seen via min(sentry.start_timestamp_precise) per group

        This is the foundation - getting aggregates per group works.
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
                Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")),
                # Total count per group
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id"),
                        label="total_count",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
                # First seen = min(timestamp_precise) per group
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_MIN,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.start_timestamp_precise"
                        ),
                        label="first_seen",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")
                    )
                ),
            ],
            limit=100,
        )

        response = EndpointTraceItemTable().execute(message)

        # Verify we got the expected columns
        assert len(response.column_values) == 3
        column_names = [col.attribute_name for col in response.column_values]
        assert "sentry.group_id" in column_names
        assert "total_count" in column_names
        assert "first_seen" in column_names

        # Verify we have results for our test groups
        group_col = next(c for c in response.column_values if c.attribute_name == "sentry.group_id")
        assert len(group_col.results) > 0

    def test_hourly_rate_with_static_divisor(self, setup_occurrence_data: dict[str, Any]) -> None:
        """
        Test calculating hourly rate with a STATIC divisor (WEEK_IN_HOURS).

        This works because the divisor is a literal, not an aggregate.
        Formula: count / WEEK_IN_HOURS
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
                Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")),
                # hourly_rate = count / WEEK_IN_HOURS (static divisor)
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_COUNT,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_INT, name="sentry.group_id"
                                ),
                                label="count",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            ),
                        ),
                        right=Column(
                            literal=Literal(val_double=float(WEEK_IN_HOURS)),
                        ),
                    ),
                    label="hourly_rate_static",
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")
                    )
                ),
            ],
            limit=100,
        )

        response = EndpointTraceItemTable().execute(message)

        # Verify we got results
        assert len(response.column_values) == 2
        rate_col = next(
            (c for c in response.column_values if c.attribute_name == "hourly_rate_static"), None
        )
        assert rate_col is not None
        assert len(rate_col.results) > 0

        # Verify rates are positive
        for result in rate_col.results:
            assert result.val_double > 0

    def test_p95_of_precomputed_attribute(self, setup_occurrence_data: dict[str, Any]) -> None:
        """
        Test P95 of a pre-computed attribute (not a calculated aggregate).

        This works because the P95 is computed over raw attribute values,
        not over a computed formula.
        """
        # Create test data with pre-computed hourly rates as attributes
        items_storage = get_storage(StorageKey("eap_items"))
        now = BASE_TIME

        # Create occurrences with explicit hourly_rate values
        rates = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
        messages = []
        for i, rate in enumerate(rates):
            group_id = 9000 + i
            messages.append(
                gen_item_message(
                    start_timestamp=now - timedelta(minutes=i),
                    type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
                    attributes={
                        "sentry.group_id": AnyValue(int_value=group_id),
                        "precomputed_hourly_rate": AnyValue(double_value=rate),
                    },
                    project_id=1,
                    remove_default_attributes=True,
                )
            )

        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        # Query P95 of the pre-computed rate
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.p95_precomputed",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id"),
                    op=ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
                    value=AttributeValue(val_int=9000),
                )
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_P95,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="precomputed_hourly_rate"
                        ),
                        label="p95_rate",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            limit=10,
        )

        response = EndpointTraceItemTable().execute(message)

        assert len(response.column_values) == 1
        p95_col = response.column_values[0]
        assert p95_col.attribute_name == "p95_rate"
        # P95 of [0.5..5.0] should be around 4.5-5.0
        p95_value = p95_col.results[0].val_double
        assert 4.0 <= p95_value <= 5.5

    def test_hourly_rate_with_dynamic_divisor_using_aggregate(
        self, setup_occurrence_data: dict[str, Any]
    ) -> None:
        """
        Test calculating hourly rate where the divisor uses an aggregate.

        Formula: count / ((now - min(timestamp_precise)) / 3600)

        This divides by a dynamically computed value that involves an aggregate (min).
        default_value_double=0.0 handles NULL/zero divisor cases safely.
        """
        now_ts = BASE_TIME.timestamp()

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.dynamic_divisor",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")),
                # hourly_rate = count / hours_since_first_seen
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(aggregation=_count_agg("event_count")),
                        right=_hours_since_first_seen(now_ts),
                        default_value_double=0.0,
                    ),
                    label="hourly_rate_dynamic",
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")
                    )
                ),
            ],
            limit=100,
        )

        response = EndpointTraceItemTable().execute(message)

        # Verify we got results
        assert len(response.column_values) == 2
        rate_col = next(
            (c for c in response.column_values if c.attribute_name == "hourly_rate_dynamic"), None
        )
        assert rate_col is not None
        assert len(rate_col.results) > 0

        # Verify rates are computed (not null) and positive
        for result in rate_col.results:
            assert not result.is_null, "Expected non-null hourly rate"
            assert result.val_double > 0, f"Expected positive rate, got {result.val_double}"

    def test_conditional_rate_based_on_aggregate_first_seen(
        self, setup_occurrence_data: dict[str, Any]
    ) -> None:
        """
        Test conditional hourly rate where the condition uses an aggregate.

        Uses ConditionalFormula to express:
        if(min(timestamp) < one_week_ago, count/168, count/hours_since_first_seen)
        """
        expected_rates = setup_occurrence_data["expected_rates"]
        one_week_ago_ts = setup_occurrence_data["one_week_ago"].timestamp()
        now_ts = setup_occurrence_data["now"].timestamp()

        conditional_formula = _conditional_rate_formula(
            one_week_ago_ts, now_ts, Column(aggregation=_count_agg())
        )

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.conditional_aggregate",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")),
                Column(
                    conditional_formula=conditional_formula,
                    label="hourly_rate",
                ),
                Column(
                    aggregation=_min_ts_agg("first_seen"),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")],
            limit=100,
        )

        response = EndpointTraceItemTable().execute(message)

        # Verify results
        group_col = next(c for c in response.column_values if c.attribute_name == "sentry.group_id")
        rate_col = next(c for c in response.column_values if c.attribute_name == "hourly_rate")
        first_seen_col = next(c for c in response.column_values if c.attribute_name == "first_seen")

        assert len(group_col.results) > 0, "Expected results for test groups"

        # Verify the conditional rate calculation
        for i, group_val in enumerate(group_col.results):
            group_id = group_val.val_int
            if group_id in expected_rates:
                first_seen = first_seen_col.results[i].val_double
                actual_rate = rate_col.results[i].val_double
                expected_rate = expected_rates[group_id]

                # Verify the rate matches expected
                assert abs(actual_rate - expected_rate) < 0.1, (
                    f"Group {group_id}: expected rate {expected_rate}, got {actual_rate}. "
                    f"first_seen={first_seen}, one_week_ago={one_week_ago_ts}"
                )

    def test_countif_combined_with_conditional_formula(
        self, setup_occurrence_data: dict[str, Any]
    ) -> None:
        """
        Test the full hourly rate calculation combining countIf with ConditionalFormula.

        This tests the complete desired calculation (steps 1-4) in a single query:
        1. countIf(timestamp > one_week_ago) via conditional_aggregation for past_week_count
        2. min(timestamp) for first_seen
        3. ConditionalFormula: if(first_seen < one_week_ago,
               past_week_count / WEEK_IN_HOURS,
               past_week_count / hours_since_first_seen)

        This demonstrates that conditional aggregation (row-level filter) works
        within ConditionalFormula branches, enabling per-group conditional rate
        calculations in a single query.
        """
        test_issues = setup_occurrence_data["test_issues"]
        one_week_ago = setup_occurrence_data["one_week_ago"]
        one_week_ago_ts = one_week_ago.timestamp()
        now = setup_occurrence_data["now"]
        now_ts = now.timestamp()

        # countIf(timestamp > one_week_ago) - only count events from the past week
        past_week_count_agg = AttributeConditionalAggregation(
            aggregate=Function.FUNCTION_COUNT,
            key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id"),
            label="past_week_count",
            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_DOUBLE, name="sentry.start_timestamp_precise"
                    ),
                    op=ComparisonFilter.OP_GREATER_THAN,
                    value=AttributeValue(val_double=one_week_ago_ts),
                )
            ),
        )

        conditional_formula = _conditional_rate_formula(
            one_week_ago_ts, now_ts, Column(conditional_aggregation=past_week_count_agg)
        )

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.countif_conditional",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")),
                Column(
                    conditional_formula=conditional_formula,
                    label="hourly_rate",
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")],
            limit=100,
        )

        response = EndpointTraceItemTable().execute(message)

        group_col = next(c for c in response.column_values if c.attribute_name == "sentry.group_id")
        rate_col = next(c for c in response.column_values if c.attribute_name == "hourly_rate")

        assert len(group_col.results) > 0, "Expected results for test groups"

        # Compute expected rates with countIf (only counts events with ts > one_week_ago)
        expected_rates: dict[int, float] = {}
        for issue in test_issues:
            group_id = issue["group_id"]
            first_seen = issue["first_seen"]
            event_count = issue["events_in_past_week"]

            if first_seen < one_week_ago:
                # Old issue: countIf excludes the first_seen event (before one_week_ago)
                past_week_count = event_count - 1
                expected_rates[group_id] = past_week_count / WEEK_IN_HOURS
            else:
                # New issue: all events are within the past week
                hours = (now - first_seen).total_seconds() / 3600
                expected_rates[group_id] = event_count / hours

        for i, group_val in enumerate(group_col.results):
            group_id = group_val.val_int
            if group_id in expected_rates:
                actual_rate = rate_col.results[i].val_double
                expected_rate = expected_rates[group_id]
                assert abs(actual_rate - expected_rate) < 0.1, (
                    f"Group {group_id}: expected rate {expected_rate:.4f}, got {actual_rate:.4f}"
                )

    def test_p95_of_hourly_rate_via_two_step_query(
        self, setup_occurrence_data: dict[str, Any]
    ) -> None:
        """
        Test P95 of calculated hourly rate across all groups via two-step approach.

        The full desired calculation (steps 1-5 from module docstring):
        1. Group by group_id
        2-4. Compute per-group conditional hourly rate (via ConditionalFormula)
        5. Compute p95 across all group rates

        Step 5 requires aggregating across group-level computed values, which
        cannot be expressed in a single query (would need subquery support).
        Instead, we query per-group rates and compute p95 client-side.
        """
        expected_rates = setup_occurrence_data["expected_rates"]
        one_week_ago_ts = setup_occurrence_data["one_week_ago"].timestamp()
        now_ts = setup_occurrence_data["now"].timestamp()

        conditional_formula = _conditional_rate_formula(
            one_week_ago_ts, now_ts, Column(aggregation=_count_agg())
        )

        # Step 1: Query per-group conditional hourly rates
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.p95_two_step",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")),
                Column(
                    conditional_formula=conditional_formula,
                    label="hourly_rate",
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")],
            limit=100,
        )

        response = EndpointTraceItemTable().execute(message)

        # Step 2: Extract per-group rates
        group_col = next(c for c in response.column_values if c.attribute_name == "sentry.group_id")
        rate_col = next(c for c in response.column_values if c.attribute_name == "hourly_rate")

        group_rates: dict[int, float] = {}
        for i, group_val in enumerate(group_col.results):
            group_id = group_val.val_int
            if group_id in expected_rates:
                group_rates[group_id] = rate_col.results[i].val_double

        assert len(group_rates) == len(expected_rates), (
            f"Expected rates for {len(expected_rates)} groups, got {len(group_rates)}"
        )

        # Verify individual rates match expected
        for group_id, expected_rate in expected_rates.items():
            actual_rate = group_rates[group_id]
            assert abs(actual_rate - expected_rate) < 0.1, (
                f"Group {group_id}: expected rate {expected_rate:.4f}, got {actual_rate:.4f}"
            )

        # Step 3: Compute p95 client-side
        rates = sorted(group_rates.values())
        n = len(rates)
        index = 0.95 * (n - 1)
        lower = int(index)
        upper = min(lower + 1, n - 1)
        frac = index - lower
        p95 = rates[lower] * (1 - frac) + rates[upper] * frac

        # Expected rates: [0.5, 0.5, 1.0, 1.0, 2.0, 2.0] -> p95 = 2.0
        assert 1.5 <= p95 <= 2.5, f"Expected p95 around 2.0, got {p95}. Rates: {rates}"
