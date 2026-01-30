"""
Tests for querying EAP with OCCURRENCE item type to calculate hourly event rates.

This test module validates both currently supported and unsupported query patterns
for calculating hourly event rates from OCCURRENCE items.

The desired hourly event rate calculation:
1. Group by group_id
2. countIf(timestamp > one_week_ago) to get past_week_event_count per group
3. min(timestamp) to find first_seen per group
4. Conditional calculation:
   - if(first_seen < one_week_ago): hourly_rate = past_week_event_count / WEEK_IN_HOURS
   - if(first_seen > one_week_ago): hourly_rate = past_week_event_count / dateDiff(hours, first_seen, now)
5. p95(hourly_rate) across all issues

Two levels of nesting that are currently NOT supported:
1. Conditional expression where the condition includes an aggregate
   (e.g., if(min(timestamp) < X, value1, value2))
2. Quantile on a calculated/computed aggregate
   (e.g., p95(count / hours) where count is an aggregate)
"""

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
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
WEEK_IN_SECONDS = WEEK_IN_HOURS * 3600


# Base time for test data - 3 hours ago to ensure data is within query window
BASE_TIME = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(
    hours=3
)

START_TIMESTAMP = Timestamp(seconds=int((BASE_TIME - timedelta(days=14)).timestamp()))
END_TIMESTAMP = Timestamp(seconds=int((BASE_TIME + timedelta(hours=1)).timestamp()))


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
        # - Rest spread over the past week
        timestamps = [first_seen]
        for i in range(event_count - 1):
            # Spread events over the past week
            event_time = now - timedelta(hours=i % WEEK_IN_HOURS)
            if event_time > first_seen:
                timestamps.append(event_time)

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
    Tests for CURRENTLY SUPPORTED query patterns.

    These tests demonstrate what CAN be done today with the EAP query system.
    """

    def test_count_and_min_timestamp_per_group(self, setup_occurrence_data: dict[str, Any]) -> None:
        """
        Test that we can:
        1. Group by sentry.group_id
        2. Count events per group
        3. Find first_seen via min(sentry.timestamp_precise) per group

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
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.timestamp_precise"
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


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestOccurrenceHourlyEventRateUnsupported(BaseApiTest):
    """
    Tests for CURRENTLY UNSUPPORTED query patterns.

    These tests document the desired query structure for features that
    are not yet implemented. They are marked xfail to indicate the
    expected failure until the features are built.

    Key unsupported features:
    1. Division by a dynamically computed value that uses an aggregate
       (e.g., count / ((now - min(timestamp)) / 3600))
    2. Conditional expression where the condition includes an aggregate
       (e.g., if(min(timestamp) < X, value1, value2))
    3. Quantile (p95) on a calculated/computed aggregate formula
       (e.g., p95 across group-level computed rates)
    """

    @pytest.mark.xfail(reason="Division by nested formula containing aggregate returns null")
    def test_hourly_rate_with_dynamic_divisor_using_aggregate(
        self, setup_occurrence_data: dict[str, Any]
    ) -> None:
        """
        Test calculating hourly rate where the divisor uses an aggregate.

        DESIRED BEHAVIOR:
        hourly_rate = count / ((now - min(timestamp_precise)) / 3600)

        This requires dividing by a dynamically computed value that
        involves an aggregate (min).

        Currently NOT fully supported because:
        - The nested formula with aggregate in the divisor returns null
        - Query executes but produces null results
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
                # where hours_since_first_seen = (now - min(timestamp)) / 3600
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_COUNT,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_INT, name="sentry.group_id"
                                ),
                                label="event_count",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            ),
                        ),
                        right=Column(
                            formula=Column.BinaryFormula(
                                op=Column.BinaryFormula.OP_DIVIDE,
                                left=Column(
                                    formula=Column.BinaryFormula(
                                        op=Column.BinaryFormula.OP_SUBTRACT,
                                        left=Column(literal=Literal(val_double=now_ts)),
                                        right=Column(
                                            aggregation=AttributeAggregation(
                                                aggregate=Function.FUNCTION_MIN,
                                                key=AttributeKey(
                                                    type=AttributeKey.TYPE_DOUBLE,
                                                    name="sentry.timestamp_precise",
                                                ),
                                                label="first_seen_ts",
                                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                                            ),
                                        ),
                                    ),
                                ),
                                right=Column(literal=Literal(val_double=3600.0)),
                            ),
                        ),
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
        # This assertion fails because nested formula with aggregate returns null
        for result in rate_col.results:
            assert not result.is_null, "Expected non-null hourly rate"
            assert result.val_double > 0, f"Expected positive rate, got {result.val_double}"

    @pytest.mark.xfail(
        reason="Conditional expression with aggregate in condition not yet supported - "
        "no 'if' function in Column.BinaryFormula to express: "
        "if(min(ts) < week_ago, count/168, count/hours_since_first_seen)"
    )
    def test_conditional_rate_based_on_aggregate_first_seen(
        self, setup_occurrence_data: dict[str, Any]
    ) -> None:
        """
        Test conditional hourly rate where the condition uses an aggregate.

        DESIRED BEHAVIOR:
        - if(min(timestamp) < one_week_ago):
            hourly_rate = count / WEEK_IN_HOURS
        - else:
            hourly_rate = count / dateDiff('hour', min(timestamp), now())

        This requires the conditional expression to accept an aggregate
        (min(timestamp)) as part of its condition evaluation.

        Currently NOT supported because:
        - There is no 'if' or conditional operator in Column.BinaryFormula
        - Only OP_ADD, OP_SUBTRACT, OP_MULTIPLY, OP_DIVIDE are available
        - Cannot express: if(aggregate < value, result1, result2)
        """
        expected_rates = setup_occurrence_data["expected_rates"]
        one_week_ago_ts = setup_occurrence_data["one_week_ago"].timestamp()
        now_ts = setup_occurrence_data["now"].timestamp()

        # This would be the desired query if 'if' was supported:
        # Column(
        #     formula=Column.ConditionalFormula(  # hypothetical
        #         condition=Column.BinaryFormula(
        #             op=Column.BinaryFormula.OP_LESS_THAN,  # hypothetical
        #             left=Column(aggregation=min(timestamp)),
        #             right=Column(literal=one_week_ago_ts),
        #         ),
        #         if_true=Column(formula=count / WEEK_IN_HOURS),
        #         if_false=Column(formula=count / hours_since_first_seen),
        #     ),
        # )

        # Since we can't express this, the test fails
        # For now, we just demonstrate we CAN compute both rates separately
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
                # Rate assuming old issue (count / WEEK_IN_HOURS)
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_COUNT,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_INT, name="sentry.group_id"
                                ),
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            ),
                        ),
                        right=Column(literal=Literal(val_double=float(WEEK_IN_HOURS))),
                    ),
                    label="rate_if_old",
                ),
                # Rate assuming new issue (count / hours_since_first_seen)
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_COUNT,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_INT, name="sentry.group_id"
                                ),
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            ),
                        ),
                        right=Column(
                            formula=Column.BinaryFormula(
                                op=Column.BinaryFormula.OP_DIVIDE,
                                left=Column(
                                    formula=Column.BinaryFormula(
                                        op=Column.BinaryFormula.OP_SUBTRACT,
                                        left=Column(literal=Literal(val_double=now_ts)),
                                        right=Column(
                                            aggregation=AttributeAggregation(
                                                aggregate=Function.FUNCTION_MIN,
                                                key=AttributeKey(
                                                    type=AttributeKey.TYPE_DOUBLE,
                                                    name="sentry.timestamp_precise",
                                                ),
                                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                                            ),
                                        ),
                                    ),
                                ),
                                right=Column(literal=Literal(val_double=3600.0)),
                            ),
                        ),
                    ),
                    label="rate_if_new",
                ),
                # First seen for reference
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_MIN,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.timestamp_precise"
                        ),
                        label="first_seen",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.group_id")],
            limit=100,
        )

        response = EndpointTraceItemTable().execute(message)

        # The test "fails" because we cannot select the correct rate based on
        # whether first_seen < one_week_ago in a single query expression
        # We would need post-processing or an 'if' function
        group_col = next(c for c in response.column_values if c.attribute_name == "sentry.group_id")
        first_seen_col = next(c for c in response.column_values if c.attribute_name == "first_seen")
        rate_old_col = next(c for c in response.column_values if c.attribute_name == "rate_if_old")
        rate_new_col = next(c for c in response.column_values if c.attribute_name == "rate_if_new")

        # Verify we can determine which rate to use for each group
        # (but we can't do this selection IN the query itself)
        for i, group_val in enumerate(group_col.results):
            group_id = group_val.val_int
            if group_id in expected_rates:
                first_seen = first_seen_col.results[i].val_double
                expected = expected_rates[group_id]

                # Determine which rate should be used
                if first_seen < one_week_ago_ts:
                    actual = rate_old_col.results[i].val_double
                else:
                    actual = rate_new_col.results[i].val_double

                # This assertion will fail because we're asserting that we
                # CAN do this selection - but we can only do it client-side
                assert abs(actual - expected) < 0.1, (
                    f"Group {group_id}: expected {expected}, got {actual}"
                )

    @pytest.mark.xfail(
        reason="P95 on calculated aggregate formula not yet supported - "
        "cannot compute p95(count/divisor) where the p95 is across groups"
    )
    def test_p95_of_calculated_hourly_rate_across_groups(
        self, setup_occurrence_data: dict[str, Any]
    ) -> None:
        """
        Test P95 of a calculated hourly rate across all groups.

        DESIRED BEHAVIOR:
        1. For each group: compute hourly_rate = count / hours
        2. Take p95 of all those hourly_rate values across groups

        This requires a nested aggregation:
        - Inner: GROUP BY group_id, compute rate per group
        - Outer: p95 across all those rates

        Currently NOT supported because:
        - Cannot express p95 over a formula result
        - Would need subquery or window function capability
        """
        # The desired query would be something like:
        # SELECT p95(hourly_rate) FROM (
        #   SELECT group_id, count(*)/168 as hourly_rate
        #   FROM occurrences
        #   GROUP BY group_id
        # )

        # We cannot express this in a single TraceItemTableRequest
        # The test demonstrates the limitation

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test.p95_calculated",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            columns=[
                # We want p95 of (count / WEEK_IN_HOURS) across groups
                # But p95 here would be computed per-row, not per-group-then-across
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_P95,
                        # This would need to reference a computed formula, not an attribute
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE,
                            name="sentry.group_id",  # Placeholder - no way to reference formula
                        ),
                        label="p95_hourly_rate",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            # No group_by - want project-wide result
            limit=10,
        )

        response = EndpointTraceItemTable().execute(message)

        # This will not give us what we want - p95 of group-level rates
        # It will give us p95 of the group_id values themselves
        assert len(response.column_values) == 1

        # The real test: verify we got a meaningful p95 of hourly rates
        # This will fail because we can't express this query
        expected_rates = list(setup_occurrence_data["expected_rates"].values())
        expected_p95 = sorted(expected_rates)[int(len(expected_rates) * 0.95)]

        actual_p95 = response.column_values[0].results[0].val_double
        # This assertion documents what we WANT - it will fail
        assert abs(actual_p95 - expected_p95) < 0.5, (
            f"Expected p95 around {expected_p95}, got {actual_p95}"
        )
