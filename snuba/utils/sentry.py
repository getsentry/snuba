from __future__ import annotations

from typing import Any

import sentry_sdk

# Stream-mode span op is an attribute, not a dedicated field.
SENTRY_OP = "sentry.op"


def set_tag_and_attribute(key: str, value: Any) -> None:
    """Dual-write a scope tag (errors) and attribute (spans) during the transition."""
    sentry_sdk.set_tag(key, value)
    sentry_sdk.set_attribute(key, value)
