"""Write- and query-path retention windows.

Valid written ``retention_days`` values are positive multiples of
:data:`RETENTION_QUANTUM`. Caps and fallbacks come from the
``retention_days`` sentry-option (standard for tier-1, downsampled for
long-term storage). There is no settings-module whitelist.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Literal, cast

from snuba.state.sentry_options import get_option

logger = logging.getLogger(__name__)

RETENTION_QUANTUM = 30
RetentionKind = Literal["standard", "downsampled"]

# Mirrors the retention_days sentry-option default. Write-path enforcement
# snaps each max down to a multiple of RETENTION_QUANTUM (396 -> 390).
# standard.default: query-path fallback when RequestMeta.standard_retention_days is unset.
# standard.max: write-path fallback / clamp for missing or over-max event retention.
# downsampled: 13 months.
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


def quantize_retention_days(value: int | None, kind: RetentionKind = "standard") -> int:
    """Snap ``value`` to a positive multiple of 30 and clamp it to ``kind``'s max.

    Missing or non-positive values become ``kind``'s max (the historical write
    default of 90 for standard). Values below one quantum become 30. Values
    above the configured max are clamped first, then quantized down.
    """
    bucket = get_retention_days_config()[kind]
    maximum = bucket["max"] // RETENTION_QUANTUM * RETENTION_QUANTUM or RETENTION_QUANTUM

    if not isinstance(value, int) or value <= 0:
        return maximum

    quantized = min(value, maximum) // RETENTION_QUANTUM * RETENTION_QUANTUM
    if quantized <= 0:
        return RETENTION_QUANTUM
    return quantized
