"""Write- and query-path retention windows.

Caps and fallbacks come from the ``retention_days`` sentry-option
(standard for tier-1, downsampled for long-term storage).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, cast

from snuba.state.sentry_options import get_option

RetentionKind = Literal["standard", "downsampled"]


def get_retention_days_config() -> dict[str, dict[str, int]]:
    """Load the nested retention_days option from sentry-options."""
    raw: object = get_option("retention_days", cast(Any, {}))
    assert isinstance(raw, Mapping)
    return {
        name: {
            "default": int(bucket["default"]),
            "max": int(bucket["max"]),
        }
        for name, bucket in raw.items()
    }


def clamp_retention_days(value: int | None, kind: RetentionKind = "standard") -> int:
    """Apply ``kind``'s default for missing/non-positive values, else clamp to max."""
    bucket = get_retention_days_config()[kind]
    if not isinstance(value, int) or value <= 0:
        return bucket["default"]
    return min(value, bucket["max"])
