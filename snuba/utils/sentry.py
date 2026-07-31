from __future__ import annotations

from typing import Any

import sentry_sdk

# Stream-mode span op lives as an attribute rather than a dedicated field.
# Centralize the key so call sites stay consistent.
SENTRY_OP = "sentry.op"


def set_tag_and_attribute(key: str, value: Any) -> None:
    """Write both a scope tag and a scope attribute.

    During the transition to attribute-based telemetry, tags still land on
    error events while attributes land on spans/logs/metrics. Write both so
    the value is available on errors and on streamed spans.
    """
    sentry_sdk.set_tag(key, value)
    sentry_sdk.set_attribute(key, value)
