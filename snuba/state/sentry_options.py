"""Thin wrapper around the ``sentry_options`` client for Snuba.

The Python counterpart to the Rust consumers' use of the ``sentry-options``
crate: both read the same read-only ``snuba`` namespace, whose schema lives in
``sentry-options/schemas/snuba/schema.json``.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, TypeVar, cast

import rapidjson
import sentry_options
from sentry_options import OptionValue

logger = logging.getLogger(__name__)

# Bound to OptionValue so ``default`` can double as both the fallback and the
# type witness for get_option / get_mapped_option below.
T = TypeVar("T", bound=OptionValue)

# Must match the directory under ``sentry-options/schemas/`` and the Rust namespace.
SNUBA_OPTIONS_NAMESPACE = "snuba"

_initialized = False


def init_options() -> None:
    """Initialize the client once per process. Idempotent and never raises, so a
    missing or misconfigured options mount can't break startup."""
    global _initialized
    if _initialized:
        return
    try:
        sentry_options.init()
        _initialized = True
    except Exception:
        logger.warning("Failed to initialize sentry-options", exc_info=True)


def get_option(key: str, default: T) -> T:
    """Read ``key`` from the Snuba namespace, returning ``default`` on any error.

    The schema declares each key's type, so the value comes back already typed;
    ``default`` doubles as the fallback and the type witness, so callers reading a
    scalar option (int/float/str/bool) get it at the right type without casting.
    """
    try:
        return cast(T, sentry_options.options(SNUBA_OPTIONS_NAMESPACE).get(key))
    except sentry_options.OptionsError:
        return default
    except Exception:
        logger.warning(
            "Unexpected error reading sentry-option %r; using default", key, exc_info=True
        )
        return default


def get_mapped_option(key: str, name: str, default: T) -> T:
    """Read entry ``name`` from a dict-typed option.

    Dynamically-named option keys (one per storage/topic/dataset) are
    collapsed into a single ``object`` option keyed by ``name``. Falls back to
    ``default`` when the option is unset, not a dict, or has no such entry.
    ``default`` doubles as the fallback and the type witness.
    """
    mapping: OptionValue = get_option(key, cast(OptionValue, {}))
    if isinstance(mapping, dict) and name in mapping:
        return cast(T, mapping[name])
    return default


def get_mapped_json_option(key: str, name: str) -> Mapping[str, Any]:
    """Read entry ``name`` from a dict-typed option whose values are JSON-object
    strings ({"clickhouse_setting": "value"}). sentry-options can't express
    dict-of-dict natively, so the second level is a JSON string parsed here.
    Returns ``{}`` when the entry is unset or isn't a JSON object."""
    raw = get_mapped_option(key, name, "")
    if not raw:
        return {}
    try:
        parsed = rapidjson.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}
