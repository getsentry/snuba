from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.types import BrokerValue, Message, Partition, Topic

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


class TestOffPeakDisabled:
    """When the feature is disabled (default), all messages pass through."""

    @patch("snuba.lw_deletions.off_peak.get_int_config", return_value=0)
    def test_messages_pass_through(self, mock_config: MagicMock) -> None:
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        strategy.submit(msg)
        next_step.submit.assert_called_once_with(msg)

    @patch("snuba.lw_deletions.off_peak.get_int_config", return_value=None)
    def test_messages_pass_through_when_config_missing(self, mock_config: MagicMock) -> None:
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()
        strategy.submit(msg)
        next_step.submit.assert_called_once_with(msg)


class TestOffPeakSameDayWindow:
    """Window like 2-8 (2am to 8am UTC)."""

    def _config(self, key: str, default: int = 0) -> int | None:
        return {
            "lw_deletions_offpeak_enabled": 1,
            "lw_deletions_offpeak_start": 2,
            "lw_deletions_offpeak_end": 8,
        }.get(key, default)

    @patch("snuba.lw_deletions.off_peak.get_int_config")
    def test_within_window_passes(self, mock_config: MagicMock) -> None:
        mock_config.side_effect = self._config
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()

        with patch("snuba.lw_deletions.off_peak.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 5, 0, tzinfo=timezone.utc)
            strategy.submit(msg)

        next_step.submit.assert_called_once_with(msg)

    @patch("snuba.lw_deletions.off_peak.get_int_config")
    def test_outside_window_rejects(self, mock_config: MagicMock) -> None:
        mock_config.side_effect = self._config
        strategy, next_step, metrics = _make_strategy()
        msg = _make_message()

        with patch("snuba.lw_deletions.off_peak.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
            with pytest.raises(MessageRejected):
                strategy.submit(msg)

        next_step.submit.assert_not_called()
        metrics.increment.assert_called_with("off_peak_rejected")

    @patch("snuba.lw_deletions.off_peak.get_int_config")
    def test_at_start_boundary_passes(self, mock_config: MagicMock) -> None:
        mock_config.side_effect = self._config
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()

        with patch("snuba.lw_deletions.off_peak.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc)
            strategy.submit(msg)

        next_step.submit.assert_called_once()

    @patch("snuba.lw_deletions.off_peak.get_int_config")
    def test_at_end_boundary_rejects(self, mock_config: MagicMock) -> None:
        mock_config.side_effect = self._config
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()

        with patch("snuba.lw_deletions.off_peak.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
            with pytest.raises(MessageRejected):
                strategy.submit(msg)


class TestOffPeakMidnightSpanningWindow:
    """Window like 22-6 (10pm to 6am UTC, spanning midnight)."""

    def _config(self, key: str, default: int = 0) -> int | None:
        return {
            "lw_deletions_offpeak_enabled": 1,
            "lw_deletions_offpeak_start": 22,
            "lw_deletions_offpeak_end": 6,
        }.get(key, default)

    @patch("snuba.lw_deletions.off_peak.get_int_config")
    def test_before_midnight_passes(self, mock_config: MagicMock) -> None:
        mock_config.side_effect = self._config
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()

        with patch("snuba.lw_deletions.off_peak.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 23, 0, tzinfo=timezone.utc)
            strategy.submit(msg)

        next_step.submit.assert_called_once()

    @patch("snuba.lw_deletions.off_peak.get_int_config")
    def test_after_midnight_passes(self, mock_config: MagicMock) -> None:
        mock_config.side_effect = self._config
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()

        with patch("snuba.lw_deletions.off_peak.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 3, 0, tzinfo=timezone.utc)
            strategy.submit(msg)

        next_step.submit.assert_called_once()

    @patch("snuba.lw_deletions.off_peak.get_int_config")
    def test_during_day_rejects(self, mock_config: MagicMock) -> None:
        mock_config.side_effect = self._config
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()

        with patch("snuba.lw_deletions.off_peak.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc)
            with pytest.raises(MessageRejected):
                strategy.submit(msg)


class TestOffPeakSameStartEnd:
    """When start == end, never off-peak (disables processing)."""

    def _config(self, key: str, default: int = 0) -> int | None:
        return {
            "lw_deletions_offpeak_enabled": 1,
            "lw_deletions_offpeak_start": 5,
            "lw_deletions_offpeak_end": 5,
        }.get(key, default)

    @patch("snuba.lw_deletions.off_peak.get_int_config")
    def test_always_rejects(self, mock_config: MagicMock) -> None:
        mock_config.side_effect = self._config
        strategy, next_step, _ = _make_strategy()
        msg = _make_message()

        with patch("snuba.lw_deletions.off_peak.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 1, 5, 0, tzinfo=timezone.utc)
            with pytest.raises(MessageRejected):
                strategy.submit(msg)


class TestOffPeakCaching:
    """Config is cached for 60 seconds."""

    @patch("snuba.lw_deletions.off_peak.time")
    @patch("snuba.lw_deletions.off_peak.get_int_config")
    def test_cache_avoids_repeated_config_reads(
        self, mock_config: MagicMock, mock_time: MagicMock
    ) -> None:
        mock_config.return_value = 0  # disabled
        mock_time.time.return_value = 1000.0

        strategy, next_step, _ = _make_strategy()
        msg = _make_message()

        strategy.submit(msg)
        strategy.submit(msg)

        # get_int_config should only be called once (for the first submit)
        assert mock_config.call_count == 1

    @patch("snuba.lw_deletions.off_peak.time")
    @patch("snuba.lw_deletions.off_peak.get_int_config")
    def test_cache_expires_after_ttl(self, mock_config: MagicMock, mock_time: MagicMock) -> None:
        mock_config.return_value = 0  # disabled
        mock_time.time.return_value = 1000.0

        strategy, next_step, _ = _make_strategy()
        msg = _make_message()

        strategy.submit(msg)
        assert mock_config.call_count == 1

        # Advance time past TTL
        mock_time.time.return_value = 1061.0
        strategy.submit(msg)
        assert mock_config.call_count == 2


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
