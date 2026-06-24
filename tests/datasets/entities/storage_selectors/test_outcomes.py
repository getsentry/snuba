from datetime import datetime, timedelta, timezone

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


_NOW = datetime.now(timezone.utc)
_OLD_START = _NOW - timedelta(days=120)  # >90 days ago
_OLD_END = _NOW - timedelta(days=90)
_RECENT_START = _NOW - timedelta(days=30)  # <90 days ago
_RECENT_END = _NOW - timedelta(days=1)

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
    connections = get_entity(EntityKey.OUTCOMES).get_all_storage_connections()

    selected = OutcomesStorageSelector().select_storage(query, settings, connections)
    assert selected.storage == expected_storage


# --- Test cases with timestamp conditions (hybrid routing) ------------------

TIMESTAMP_CASES = [
    # >90 days ago -> daily, regardless of referrer
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
    # <90 days ago -> referrer fallback
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
]


@pytest.mark.parametrize("query, settings, expected_storage", TIMESTAMP_CASES)
def test_storage_selector_with_timestamps(
    query: Query,
    settings: HTTPQuerySettings,
    expected_storage: Storage,
) -> None:
    """
    Hybrid routing: time-range check takes priority over referrer.

    - Query start >90 days ago -> daily (hourly table lacks the data).
    - Query start <90 days ago + billing referrer -> daily (referrer fallback).
    - Query start <90 days ago + non-billing referrer -> hourly.
    """
    connections = get_entity(EntityKey.OUTCOMES).get_all_storage_connections()

    selected = OutcomesStorageSelector().select_storage(query, settings, connections)
    assert selected.storage == expected_storage
