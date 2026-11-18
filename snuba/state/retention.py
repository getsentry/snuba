"""Write- and query-path retention windows.

Caps and fallbacks come from the ``retention_days`` sentry-option
(standard for tier-1, downsampled for long-term storage).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Literal, cast

from snuba.state.sentry_options import get_option

logger = logging.getLogger(__name__)

RetentionKind = Literal["standard", "downsampled"]

# Mirrors the retention_days sentry-option default so a missing or empty option
# cannot crash write- or query-path clamping.
DEFAULT_RETENTION_DAYS: dict[str, dict[str, int]] = {
    "standard": {"default": 30, "max": 90},
    "downsampled": {"default": 396, "max": 396},
}


def _positive_int(value: object, fallback: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return fallback
    return value


def _retention_bucket(config: Mapping[str, object], name: str) -> dict[str, int]:
    fallback = DEFAULT_RETENTION_DAYS[name]
    raw = config.get(name, fallback)
    if not isinstance(raw, Mapping):
        return dict(fallback)
    return {
        "default": _positive_int(raw.get("default"), fallback["default"]),
        "max": _positive_int(raw.get("max"), fallback["max"]),
    }


def get_retention_days_config() -> dict[str, dict[str, int]]:
    """Load the nested retention_days option, falling back to code defaults."""
    # Nested object option; cast because OptionValue's static type is only one level deep.
    raw: object = get_option("retention_days", cast(Any, DEFAULT_RETENTION_DAYS))
    if not isinstance(raw, Mapping):
        logger.warning("Invalid retention_days option %r; using defaults", raw)
        return {name: dict(bucket) for name, bucket in DEFAULT_RETENTION_DAYS.items()}

    return {name: _retention_bucket(raw, name) for name in DEFAULT_RETENTION_DAYS}


def clamp_retention_days(value: int | None, kind: RetentionKind = "standard") -> int:
    """Apply ``kind``'s default for missing/non-positive values, else clamp to max."""
    bucket = get_retention_days_config()[kind]
    if not isinstance(value, int) or value <= 0:
        return bucket["default"]
    return min(value, bucket["max"])


# EAP items don't read the retention_days option, which only applies to non-EAP
# data. These mirror the Rust consumer's EAP constants (rust_snuba/src/processors/utils.rs).
EAP_DEFAULT_RETENTION_DAYS = 30
EAP_MAX_RETENTION_DAYS = 396


def eap_standard_retention_days(requested: int | None) -> int:
    """Query-path standard retention for EAP: the requested value capped at
    ``EAP_MAX_RETENTION_DAYS``, or ``EAP_DEFAULT_RETENTION_DAYS`` if unset."""
    if not isinstance(requested, int) or requested <= 0:
        return EAP_DEFAULT_RETENTION_DAYS
    return min(requested, EAP_MAX_RETENTION_DAYS)
