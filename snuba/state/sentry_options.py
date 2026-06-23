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
        return default
