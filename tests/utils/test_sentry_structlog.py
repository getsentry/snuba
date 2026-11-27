import logging
from unittest.mock import MagicMock, patch

from snuba.utils.sentry_structlog import SentryProcessor


@patch("snuba.utils.sentry_structlog.sentry_sdk")
def test_error_creates_event_and_breadcrumb(sentry_sdk: MagicMock) -> None:
    sentry_sdk.capture_event.return_value = "abc123"

    result = SentryProcessor(event_level=logging.ERROR)(
        MagicMock(), "error", {"event": "failed", "level": "error", "module": "snuba.test"}
    )

    event = sentry_sdk.capture_event.call_args.args[0]
    assert event["message"] == "failed"
    assert event["level"] == "error"
    assert event["contexts"]["structlog"]["module"] == "snuba.test"
    assert "sentry_id" not in event["contexts"]["structlog"]
    assert result["sentry_id"] == "abc123"
    sentry_sdk.add_breadcrumb.assert_called_once()


@patch("snuba.utils.sentry_structlog.sentry_sdk")
def test_warning_only_creates_breadcrumb(sentry_sdk: MagicMock) -> None:
    SentryProcessor(event_level=logging.ERROR)(
        MagicMock(), "warning", {"event": "slow", "level": "warning"}
    )

    sentry_sdk.capture_event.assert_not_called()
    sentry_sdk.add_breadcrumb.assert_called_once()


@patch("snuba.utils.sentry_structlog.sentry_sdk")
def test_debug_is_not_reported(sentry_sdk: MagicMock) -> None:
    SentryProcessor(event_level=logging.ERROR)(
        MagicMock(), "debug", {"event": "noise", "level": "debug"}
    )

    sentry_sdk.capture_event.assert_not_called()
    sentry_sdk.add_breadcrumb.assert_not_called()
