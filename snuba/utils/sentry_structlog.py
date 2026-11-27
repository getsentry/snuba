from __future__ import annotations

import logging

import sentry_sdk
from sentry_sdk.types import Event
from sentry_sdk.utils import capture_internal_exceptions
from structlog.types import EventDict, WrappedLogger


class SentryProcessor:
    """
    Report structlog entries to Sentry as events and breadcrumbs.

    Replaces structlog-sentry, which doesn't support sentry-sdk 3.x. This runs
    after ``format_exc_info``, so exceptions arrive as a string in the
    ``structlog`` context rather than as ``exc_info``.
    """

    def __init__(self, event_level: int, breadcrumb_level: int = logging.INFO) -> None:
        self.event_level = event_level
        self.breadcrumb_level = breadcrumb_level

    def __call__(self, logger: WrappedLogger, name: str, event_dict: EventDict) -> EventDict:
        level = getattr(logging, event_dict["level"].upper())
        context = dict(event_dict)

        if level >= self.event_level:
            with capture_internal_exceptions():
                event: Event = {
                    "message": event_dict["event"],
                    "level": event_dict["level"],
                    "contexts": {"structlog": context},
                }
                if "logger" in event_dict:
                    event["logger"] = event_dict["logger"]
                event_id = sentry_sdk.capture_event(event)
                if event_id:
                    event_dict["sentry_id"] = event_id

        if level >= self.breadcrumb_level:
            with capture_internal_exceptions():
                sentry_sdk.add_breadcrumb(
                    {
                        "type": "log",
                        "level": event_dict["level"],
                        "category": event_dict.get("logger"),
                        "message": event_dict["event"],
                        "timestamp": event_dict.get("timestamp"),
                        "data": {},
                    },
                    hint={"log_record": event_dict},
                )

        return event_dict
