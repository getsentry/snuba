from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import time_machine
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba import state
from snuba.lw_deletions.off_peak import OffPeakProcessingStrategy


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

    def setup_method(self) -> None:
        state.set_config("lw_deletions_offpeak_enabled", 1)
        state.set_config("lw_deletions_offpeak_start", 2)
        state.set_config("lw_deletions_offpeak_end", 8)

    @time_machine.travel(datetime(2026, 1, 1, 5, 0, tzinfo=timezone.utc), tick=False)
    def test_within_window_passes(self) -> None:
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        strategy.submit(msg)
        next_step.submit.assert_called_once_with(msg)

    @time_machine.travel(datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc), tick=False)
    def test_outside_window_rejects(self) -> None:
        strategy, next_step, metrics = _make_strategy()
        msg = _make_message()
        with pytest.raises(MessageRejected):
            strategy.submit(msg)
        next_step.submit.assert_not_called()
        metrics.increment.assert_called_with("off_peak_rejected")

    @time_machine.travel(datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc), tick=False)
    def test_at_start_boundary_passes(self) -> None:
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        strategy.submit(msg)
        next_step.submit.assert_called_once()

    @time_machine.travel(datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc), tick=False)
    def test_at_end_boundary_rejects(self) -> None:
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        with pytest.raises(MessageRejected):
            strategy.submit(msg)


@pytest.mark.redis_db
class TestOffPeakMidnightSpanningWindow:
    """Window like 22-6 (10pm to 6am UTC, spanning midnight)."""

    def setup_method(self) -> None:
        state.set_config("lw_deletions_offpeak_enabled", 1)
        state.set_config("lw_deletions_offpeak_start", 22)
        state.set_config("lw_deletions_offpeak_end", 6)

    @time_machine.travel(datetime(2026, 1, 1, 23, 0, tzinfo=timezone.utc), tick=False)
    def test_before_midnight_passes(self) -> None:
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        strategy.submit(msg)
        next_step.submit.assert_called_once()

    @time_machine.travel(datetime(2026, 1, 1, 3, 0, tzinfo=timezone.utc), tick=False)
    def test_after_midnight_passes(self) -> None:
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        strategy.submit(msg)
        next_step.submit.assert_called_once()

    @time_machine.travel(datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc), tick=False)
    def test_during_day_rejects(self) -> None:
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        with pytest.raises(MessageRejected):
            strategy.submit(msg)


@pytest.mark.redis_db
class TestOffPeakSameStartEnd:
    """When start == end, never off-peak (disables processing)."""

    @time_machine.travel(datetime(2026, 1, 1, 5, 0, tzinfo=timezone.utc), tick=False)
    def test_always_rejects(self) -> None:
        state.set_config("lw_deletions_offpeak_enabled", 1)
        state.set_config("lw_deletions_offpeak_start", 5)
        state.set_config("lw_deletions_offpeak_end", 5)
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        with pytest.raises(MessageRejected):
            strategy.submit(msg)


@pytest.mark.redis_db
class TestOffPeakCaching:
    """Config is cached for 60 seconds."""

    @time_machine.travel(datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc), tick=False)
    def test_config_change_takes_effect_after_cache_expires(self) -> None:
        state.set_config("lw_deletions_offpeak_enabled", 1)
        state.set_config("lw_deletions_offpeak_start", 2)
        state.set_config("lw_deletions_offpeak_end", 8)

        strategy, next_step, _ = _make_strategy()
        msg = _make_message()

        # Hour 12 is outside 2-8, should reject
        with pytest.raises(MessageRejected):
            strategy.submit(msg)

        # Disable the feature — but cache hasn't expired yet
        state.set_config("lw_deletions_offpeak_enabled", 0)
        with pytest.raises(MessageRejected):
            strategy.submit(msg)

        # Expire the cache by resetting internal state
        strategy._OffPeakProcessingStrategy__cached_at = 0.0  # type: ignore[attr-defined]

        # Now the disabled config takes effect
        strategy.submit(msg)
        next_step.submit.assert_called_once()


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
