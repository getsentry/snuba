"""Thin wrapper around the ``sentry_options`` client for Snuba.

The Python counterpart to the Rust consumers' use of the ``sentry-options``
crate: both read the same read-only ``snuba`` namespace, whose schema lives in
``sentry-options/schemas/snuba/schema.json``.
"""

from __future__ import annotations

import enum
import logging
from typing import Final, TypeVar, cast, overload

import sentry_options
from sentry_options import OptionValue

logger = logging.getLogger(__name__)

# Bound to OptionValue so ``default`` can double as both the fallback and the
# type witness for get_option / get_mapped_option below.
T = TypeVar("T", bound=OptionValue)


class _Missing(enum.Enum):
    MISSING = enum.auto()


# Sentinel for "no caller fallback": get_option then relies on the schema default.
_MISSING: Final = _Missing.MISSING

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


@overload
def get_option(key: str) -> OptionValue: ...


@overload
def get_option(key: str, default: T) -> T: ...


def get_option(key: str, default: T | _Missing = _MISSING) -> T | OptionValue:
    """Read ``key`` from the Snuba namespace.

    The schema declares each key's type and default, so the value comes back
    already typed. ``default`` is optional:

    - Without it, the schema default is the only default and any error reading
      the option is raised, so a misconfigured client or a key missing from the
      schema fails loudly instead of silently using a value from code.
    - With it, ``default`` is returned on any error and doubles as the type
      witness, so callers reading a scalar option (int/float/str/bool) get it at
      the right type without casting.
    """
    try:
        return sentry_options.options(SNUBA_OPTIONS_NAMESPACE).get(key)
    except sentry_options.OptionsError:
        if isinstance(default, _Missing):
            raise
        return default
    except Exception:
        if isinstance(default, _Missing):
            raise
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
