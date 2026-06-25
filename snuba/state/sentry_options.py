"""Thin wrapper around the ``sentry_options`` client for Snuba.

This is the Python counterpart to the Rust consumers' use of the
``sentry-options`` crate (see ``rust_snuba/src/strategies/blq_router.rs``).
Both read the same ``snuba`` namespace, whose schema lives in
``sentry-options/schemas/snuba/schema.json`` and whose values are managed in
sentry-options-automator and delivered as volume-mounted JSON.

Unlike runtime config (``snuba.state.get_config`` and friends, backed by
Redis), sentry-options values are read-only from Snuba's perspective: they are
edited centrally and synced into the process, with no in-Snuba write path.
"""

from __future__ import annotations

import logging

import sentry_options
from sentry_options import OptionValue

logger = logging.getLogger(__name__)

# Namespace Snuba registers with sentry-options. Must match the directory name
# under ``sentry-options/schemas/`` and the namespace the Rust consumers use.
SNUBA_OPTIONS_NAMESPACE = "snuba"

_initialized = False


def init_options() -> None:
    """Initialize the sentry-options client once per process.

    Schemas and values are discovered via the ``sentry_options`` fallback chain
    (the ``SENTRY_OPTIONS_DIR`` env var, then ``/etc/sentry-options``, then
    ``./sentry-options``). Safe to call repeatedly; only the first successful
    call does any work.

    Failures are logged but never raised: a missing or misconfigured options
    mount must not take down a service at startup. When initialization fails,
    :func:`get_option` falls back to the default passed by each call site, so
    behavior matches the pre-sentry-options world.
    """
    global _initialized
    if _initialized:
        return
    try:
        sentry_options.init()
        _initialized = True
    except Exception:
        logger.warning("Failed to initialize sentry-options", exc_info=True)


def get_option(key: str, default: OptionValue) -> OptionValue:
    """Read ``key`` from the Snuba sentry-options namespace.

    Returns the configured value, or the schema default when no value is set.
    If sentry-options is unavailable for any reason — not initialized, unknown
    option, or any other client error — ``default`` is returned, so call sites
    behave exactly as they did before the option existed.
    """
    try:
        return sentry_options.options(SNUBA_OPTIONS_NAMESPACE).get(key)
    except sentry_options.OptionsError:
        # Expected fallbacks: the client never initialized
        # (NotInitializedError), the option/namespace is unknown, or the
        # schema is invalid. These all subclass OptionsError; return the
        # call-site default silently so behavior matches the pre-option world.
        return default
    except Exception:
        # The client should only ever raise OptionsError, but a hot query path
        # must never crash on a config read: honor the "any reason" contract
        # above and log the unexpected error so it is still noticed.
        logger.warning(
            "Unexpected error reading sentry-option %r; using default", key, exc_info=True
        )
        return default


def _coerce_bool(value: OptionValue, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return default


def _coerce_int(value: OptionValue, default: int) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, str)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    return default


def _coerce_float(value: OptionValue, default: float) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float, str)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    return default


def _coerce_str(value: OptionValue, default: str) -> str:
    if isinstance(value, str):
        return value
    return default


def get_bool_option(key: str, default: bool) -> bool:
    """Read ``key`` as a bool. Replaces ``state.get_int_config`` used as a flag.

    The schema type for these keys is ``boolean``, so ``get`` returns a real
    ``bool``; the int/str coercion only guards against a misconfigured value
    and otherwise falls back to ``default``.
    """
    return _coerce_bool(get_option(key, default), default)


def get_int_option(key: str, default: int) -> int:
    """Read ``key`` as an int. Counterpart to ``state.get_int_config``."""
    return _coerce_int(get_option(key, default), default)


def get_float_option(key: str, default: float) -> float:
    """Read ``key`` as a float. Counterpart to ``state.get_float_config``."""
    return _coerce_float(get_option(key, default), default)


def get_str_option(key: str, default: str) -> str:
    """Read ``key`` as a str. Counterpart to ``state.get_str_config``."""
    return _coerce_str(get_option(key, default), default)


def get_mapped_option(key: str, name: str, default: OptionValue) -> OptionValue:
    """Read one entry from a dict-typed option keyed by a dynamic ``name``.

    Some runtime-config keys were named dynamically — one Redis key per
    storage, topic, dataset, or bucket (``f"{prefix}_{name}"``). A static
    sentry-options schema cannot enumerate those, so the migration collapses
    each family into a single ``object`` option ``key`` — a dictionary declared
    with ``additionalProperties`` and defaulting to ``{}`` — whose value maps
    the dynamic ``name`` to its value.

    Returns the entry for ``name``; falls back to ``default`` when the option
    is unset/empty, is not a dictionary, or has no entry for ``name``. Because
    a dict option allows arbitrary keys of the declared value type, the typed
    wrappers below still coerce the entry defensively.
    """
    mapping = get_option(key, {})
    if isinstance(mapping, dict) and name in mapping:
        return mapping[name]
    return default


def get_mapped_int_option(key: str, name: str, default: int) -> int:
    """``get_int_option`` for one entry of a JSON-object option (see
    :func:`get_mapped_option`)."""
    return _coerce_int(get_mapped_option(key, name, default), default)


def get_mapped_float_option(key: str, name: str, default: float) -> float:
    """``get_float_option`` for one entry of a JSON-object option (see
    :func:`get_mapped_option`)."""
    return _coerce_float(get_mapped_option(key, name, default), default)


def get_mapped_str_option(key: str, name: str, default: str) -> str:
    """``get_str_option`` for one entry of a JSON-object option (see
    :func:`get_mapped_option`)."""
    return _coerce_str(get_mapped_option(key, name, default), default)
