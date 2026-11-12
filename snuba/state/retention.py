"""Write- and query-path retention windows.

Caps and fallbacks come from the ``retention_days`` sentry-option
(standard for tier-1, downsampled for long-term storage).

Only ``SUPPORTED_RETENTION_DAYS`` may be written, for both standard and
downsampled retention. Tables are partitioned by
``(retention_days, toMonday(timestamp))``, so every distinct value adds a full
set of partitions. Any other value is rejected, never rounded.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Literal, cast

from snuba import environment
from snuba.state.sentry_options import get_option
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)
_consumer_metrics = MetricsWrapper(environment.metrics, "consumer")

RetentionKind = Literal["standard", "downsampled"]

# Mirrors the retention_days sentry-option default so a missing or empty option
# cannot crash write- or query-path clamping.
DEFAULT_RETENTION_DAYS: dict[str, dict[str, int]] = {
    "standard": {"default": 30, "max": 396},
    "downsampled": {"default": 396, "max": 396},
}


# Retention values that may be written. Partitions only ever exist for these.
SUPPORTED_RETENTION_DAYS: frozenset[int] = frozenset({7, 30, 60, 90, 180, 366, 396})

# Counter for messages dropped for an unsupported retention value. Shared with
# the Rust consumers (snuba.consumer.<name>); the ``processor`` tag says which one.
DROPPED_UNSUPPORTED_RETENTION_METRIC = "dropped_unsupported_retention"


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


def enforce_retention_days(value: int | None, kind: RetentionKind = "standard") -> int | None:
    """Write-path retention for ``kind``.

    Missing or non-positive values get the configured default. A supported
    value up to the configured max is kept as is. Any other value (unsupported
    or above max) returns ``None``: the message must not be ingested.
    """
    bucket = get_retention_days_config()[kind]
    if not isinstance(value, int) or value <= 0:
        return bucket["default"]
    if value not in SUPPORTED_RETENTION_DAYS or value > bucket["max"]:
        return None
    return value


def record_unsupported_retention(processor: str) -> None:
    """Count a message dropped by ``processor`` for an unsupported retention value."""
    _consumer_metrics.increment(DROPPED_UNSUPPORTED_RETENTION_METRIC, tags={"processor": processor})
