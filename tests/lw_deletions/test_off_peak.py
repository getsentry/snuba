from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
import time_machine
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba import state
from snuba.lw_deletions.off_peak import OffPeakProcessingStrategy
from snuba.state import get_raw_configs


@pytest.fixture(autouse=True)
def _clear_state_memoize_cache() -> None:
    """The memoize on get_raw_configs uses time.time() for TTL which
    conflicts with time_machine. Clear it between tests."""
    for cell in get_raw_configs.__closure__ or ():  # type: ignore[attr-defined]
        obj = cell.cell_contents
        if hasattr(obj, "saved"):
            obj.saved.clear()
            obj.at.clear()
            break


def _tomorrow_at(hour: int) -> datetime:
    """Return tomorrow at the given UTC hour. Always in the future so
    time_machine.travel moves the clock forward and the snuba.state
    memoize cache naturally expires."""
    tomorrow = datetime.now(timezone.utc).date() + timedelta(days=1)
    return datetime(tomorrow.year, tomorrow.month, tomorrow.day, hour, tzinfo=timezone.utc)


def _make_message() -> Message[KafkaPayload]:
    return Message(
        BrokerValue(
            KafkaPayload(None, b'{"rows_to_delete": 1}', []),
            Partition(Topic("test"), 0),
            0,
            datetime(1970, 1, 1),
        )
    )


def _make_strategy(
    next_step: MagicMock | None = None,
) -> tuple[OffPeakProcessingStrategy, MagicMock, MagicMock]:
    next_step = next_step or MagicMock()
    metrics = MagicMock()
    strategy = OffPeakProcessingStrategy(next_step=next_step, metrics=metrics)
    return strategy, next_step, metrics


def _set_offpeak_config(start: int, end: int) -> None:
    state.set_config("lw_deletions_offpeak_enabled", 1)
    state.set_config("lw_deletions_offpeak_start", start)
    state.set_config("lw_deletions_offpeak_end", end)


@pytest.mark.redis_db
class TestOffPeakDisabled:
    """When the feature is disabled (default), all messages pass through."""

    def test_messages_pass_through(self) -> None:
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        strategy.submit(msg)
        next_step.submit.assert_called_once_with(msg)

    def test_messages_pass_through_when_explicitly_disabled(self) -> None:
        state.set_config("lw_deletions_offpeak_enabled", 0)
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        strategy.submit(msg)
        next_step.submit.assert_called_once_with(msg)


@pytest.mark.redis_db
class TestOffPeakSameDayWindow:
    """Window like 2-8 (2am to 8am UTC)."""

    def test_within_window_passes(self) -> None:
        with time_machine.travel(_tomorrow_at(5), tick=False):
            _set_offpeak_config(start=2, end=8)
            strategy, next_step, _ = _make_strategy()
            strategy.submit(_make_message())
            next_step.submit.assert_called_once()

    def test_outside_window_rejects(self) -> None:
        with time_machine.travel(_tomorrow_at(12), tick=False):
            _set_offpeak_config(start=2, end=8)
            strategy, next_step, metrics = _make_strategy()
            with pytest.raises(MessageRejected):
                strategy.submit(_make_message())
            next_step.submit.assert_not_called()
            metrics.increment.assert_called_with("off_peak_rejected")

    def test_at_start_boundary_passes(self) -> None:
        with time_machine.travel(_tomorrow_at(2), tick=False):
            _set_offpeak_config(start=2, end=8)
            strategy, next_step, _ = _make_strategy()
            strategy.submit(_make_message())
            next_step.submit.assert_called_once()

    def test_at_end_boundary_rejects(self) -> None:
        with time_machine.travel(_tomorrow_at(8), tick=False):
            _set_offpeak_config(start=2, end=8)
            strategy, next_step, _ = _make_strategy()
            with pytest.raises(MessageRejected):
                strategy.submit(_make_message())


@pytest.mark.redis_db
class TestOffPeakMidnightSpanningWindow:
    """Window like 22-6 (10pm to 6am UTC, spanning midnight)."""

    def test_before_midnight_passes(self) -> None:
        with time_machine.travel(_tomorrow_at(23), tick=False):
            _set_offpeak_config(start=22, end=6)
            strategy, next_step, _ = _make_strategy()
            strategy.submit(_make_message())
            next_step.submit.assert_called_once()

    def test_after_midnight_passes(self) -> None:
        with time_machine.travel(_tomorrow_at(3), tick=False):
            _set_offpeak_config(start=22, end=6)
            strategy, next_step, _ = _make_strategy()
            strategy.submit(_make_message())
            next_step.submit.assert_called_once()

    def test_during_day_rejects(self) -> None:
        with time_machine.travel(_tomorrow_at(14), tick=False):
            _set_offpeak_config(start=22, end=6)
            strategy, next_step, _ = _make_strategy()
            with pytest.raises(MessageRejected):
                strategy.submit(_make_message())


@pytest.mark.redis_db
class TestOffPeakSameStartEnd:
    """When start == end, never off-peak (disables processing)."""

    def test_always_rejects(self) -> None:
        with time_machine.travel(_tomorrow_at(5), tick=False):
            _set_offpeak_config(start=5, end=5)
            strategy, next_step, _ = _make_strategy()
            with pytest.raises(MessageRejected):
                strategy.submit(_make_message())


class TestOffPeakDelegation:
    """Lifecycle methods delegate to next_step."""

    def test_poll_delegates(self) -> None:
        strategy, next_step, _ = _make_strategy()
        strategy.poll()
        next_step.poll.assert_called_once()

    def test_close_delegates(self) -> None:
        strategy, next_step, _ = _make_strategy()
        strategy.close()
        next_step.close.assert_called_once()

    def test_terminate_delegates(self) -> None:
        strategy, next_step, _ = _make_strategy()
        strategy.terminate()
        next_step.terminate.assert_called_once()

    def test_join_delegates(self) -> None:
        strategy, next_step, _ = _make_strategy()
        strategy.join(timeout=5.0)
        next_step.join.assert_called_once_with(5.0)
