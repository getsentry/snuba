from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entities.storage_selectors.outcomes import OutcomesStorageSelector
from snuba.datasets.storage import Storage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.conditions import BooleanFunctions, ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings, OutcomesQuerySettings

OUTCOMES_ENTITY = Entity(
    key=EntityKey("outcomes"),
    schema=get_entity(EntityKey("outcomes")).get_data_model(),
    sample=None,
)
DAILY = get_storage(StorageKey.OUTCOMES_DAILY)
HOURLY = get_storage(StorageKey.OUTCOMES_HOURLY)

# Frozen so cutoff / boundary cases do not drift with wall-clock time.
# Cutoff is now - 90d + 1d = now - 89d (inclusive).
_NOW = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)
_CUTOFF = _NOW - timedelta(days=89)
_OLD_START = _NOW - timedelta(days=120)
_OLD_END = _NOW - timedelta(days=90)
_RECENT_START = _NOW - timedelta(days=30)
_RECENT_END = _NOW - timedelta(days=1)
_JUST_INSIDE_HOURLY = _CUTOFF + timedelta(seconds=1)
_NAIVE_OLD_START = datetime(2025, 12, 24, 12, 0, 0)  # 120 days before _NOW, no tz


def _make_timestamp_condition(start: datetime, end: datetime) -> FunctionCall:
    """Build a ``timestamp >= start AND timestamp < end`` condition node."""
    return binary_condition(
        BooleanFunctions.AND,
        binary_condition(
            ConditionFunctions.GTE,
            Column(None, None, "timestamp"),
            Literal(None, start),
        ),
        binary_condition(
            ConditionFunctions.LT,
            Column(None, None, "timestamp"),
            Literal(None, end),
        ),
    )


def _query_with_timestamps(start: datetime, end: datetime) -> Query:
    """Return a Query whose WHERE clause contains a timestamp range."""
    return Query(
        from_clause=OUTCOMES_ENTITY,
        condition=_make_timestamp_condition(start, end),
    )


def _select(query: Query, settings: HTTPQuerySettings) -> Storage:
    connections = get_entity(EntityKey.OUTCOMES).get_all_storage_connections()
    return OutcomesStorageSelector().select_storage(query, settings, connections).storage


# --- Test cases without timestamp conditions (query is irrelevant) ----------

NO_TIMESTAMP_CASES = [
    pytest.param(OutcomesQuerySettings(), HOURLY, id="outcomes_settings_default_hourly"),
    pytest.param(OutcomesQuerySettings(use_daily=True), DAILY, id="outcomes_settings_use_daily"),
    pytest.param(OutcomesQuerySettings(use_daily=False), HOURLY, id="outcomes_settings_no_daily"),
    pytest.param(HTTPQuerySettings(), HOURLY, id="no_timestamp_default_hourly"),
    pytest.param(
        HTTPQuerySettings(referrer="outcomes.timeseries"),
        HOURLY,
        id="no_timestamp_non_billing_hourly",
    ),
    pytest.param(
        HTTPQuerySettings(referrer="billing.usage_service.clickhouse"),
        DAILY,
        id="no_timestamp_billing_referrer_daily",
    ),
    pytest.param(
        HTTPQuerySettings(referrer="billing.anything"),
        DAILY,
        id="no_timestamp_billing_prefix_daily",
    ),
]


@pytest.mark.parametrize("settings, expected_storage", NO_TIMESTAMP_CASES)
def test_storage_selector_no_timestamp(
    settings: HTTPQuerySettings,
    expected_storage: Storage,
) -> None:
    """
    Routing without timestamp conditions in the query.

    - OutcomesQuerySettings with use_daily=True -> daily.
    - Referrers starting with "billing." -> daily (13-month retention).
    - Everything else -> hourly.
    """
    query = Query(from_clause=OUTCOMES_ENTITY)
    assert _select(query, settings) == expected_storage


# --- Test cases with timestamp conditions (hybrid routing) ------------------

TIMESTAMP_CASES = [
    # Beyond hourly retention -> daily, regardless of referrer
    pytest.param(
        _query_with_timestamps(_OLD_START, _OLD_END),
        HTTPQuerySettings(referrer="outcomes.timeseries"),
        DAILY,
        id="old_range_non_billing_daily",
    ),
    pytest.param(
        _query_with_timestamps(_OLD_START, _OLD_END),
        HTTPQuerySettings(referrer="billing.anything"),
        DAILY,
        id="old_range_billing_daily",
    ),
    # Inside hourly retention -> referrer fallback
    pytest.param(
        _query_with_timestamps(_RECENT_START, _RECENT_END),
        HTTPQuerySettings(referrer="billing.anything"),
        DAILY,
        id="recent_range_billing_daily",
    ),
    pytest.param(
        _query_with_timestamps(_RECENT_START, _RECENT_END),
        HTTPQuerySettings(referrer="outcomes.timeseries"),
        HOURLY,
        id="recent_range_non_billing_hourly",
    ),
    # Inclusive cutoff: start == now - 89d routes to daily
    pytest.param(
        _query_with_timestamps(_CUTOFF, _NOW),
        HTTPQuerySettings(referrer="outcomes.timeseries"),
        DAILY,
        id="cutoff_inclusive_daily",
    ),
    # Just inside the buffered hourly window stays on hourly
    pytest.param(
        _query_with_timestamps(_JUST_INSIDE_HOURLY, _NOW),
        HTTPQuerySettings(referrer="outcomes.timeseries"),
        HOURLY,
        id="just_inside_hourly",
    ),
    # SNQL datetime literals are naive; still route on time range
    pytest.param(
        _query_with_timestamps(_NAIVE_OLD_START, _OLD_END.replace(tzinfo=None)),
        HTTPQuerySettings(referrer="outcomes.timeseries"),
        DAILY,
        id="naive_old_range_daily",
    ),
]


@pytest.mark.parametrize("query, settings, expected_storage", TIMESTAMP_CASES)
@patch(
    "snuba.datasets.entities.storage_selectors.outcomes.datetime",
    wraps=datetime,
)
def test_storage_selector_with_timestamps(
    mock_datetime: object,
    query: Query,
    settings: HTTPQuerySettings,
    expected_storage: Storage,
) -> None:
    """
    Hybrid routing: time-range check takes priority over referrer.

    - Query start at or beyond the buffered 90-day cutoff -> daily.
    - Query start inside the hourly window + billing referrer -> daily.
    - Query start inside the hourly window + non-billing referrer -> hourly.
    """
    mock_datetime.now.return_value = _NOW  # type: ignore[attr-defined]
    assert _select(query, settings) == expected_storage


@patch(
    "snuba.datasets.entities.storage_selectors.outcomes.datetime",
    wraps=datetime,
)
def test_use_daily_false_falls_through_to_time_range(mock_datetime: object) -> None:
    """use_daily=False is not an opt-out; old windows still go to daily."""
    mock_datetime.now.return_value = _NOW  # type: ignore[attr-defined]
    query = _query_with_timestamps(_OLD_START, _OLD_END)
    assert _select(query, OutcomesQuerySettings(use_daily=False)) == DAILY


@patch(
    "snuba.datasets.entities.storage_selectors.outcomes.datetime",
    wraps=datetime,
)
def test_use_daily_true_wins_over_recent_range(mock_datetime: object) -> None:
    """use_daily=True is an explicit opt-in even for recent windows."""
    mock_datetime.now.return_value = _NOW  # type: ignore[attr-defined]
    query = _query_with_timestamps(_RECENT_START, _RECENT_END)
    assert _select(query, OutcomesQuerySettings(use_daily=True)) == DAILY
