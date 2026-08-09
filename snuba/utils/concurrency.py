from __future__ import annotations

import os
from functools import lru_cache

_DEFAULT_QUERY_CONCURRENCY = 8

# Bounds the damage a bogus env value can do, since pool sizes derive from this.
_MAX_QUERY_CONCURRENCY = 128


def _positive_int_env(name: str) -> int | None:
    """Read ``name`` as a positive int, or ``None`` if unset/malformed/non-positive."""
    raw = os.environ.get(name)
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def declare_query_concurrency(concurrency: int) -> None:
    """Record this process's query concurrency for pool sizing.

    For entry points that know their own thread count but are not launched
    through the ``granian`` CLI, so none of the ``GRANIAN_*`` env vars that
    :func:`process_query_concurrency` reads are set: the ``snuba api`` /
    ``snuba admin`` commands (which construct ``Granian`` programmatically) and
    the subscription executors (which run their own ``ThreadPoolExecutor``).
    Without this they fall through to ``_DEFAULT_QUERY_CONCURRENCY`` and size
    their pools well below the number of threads actually running queries.

    Must be called before the first ClickHouse pool is created. An explicitly
    set ``SNUBA_QUERY_CONCURRENCY`` stays authoritative.
    """
    os.environ.setdefault("SNUBA_QUERY_CONCURRENCY", str(max(1, concurrency)))
    # The value is cached on first read, so drop it here rather than leave this
    # silently dependent on nothing having asked yet.
    process_query_concurrency.cache_clear()


@lru_cache(maxsize=1)
def process_query_concurrency() -> int:
    """
    The maximum number of ClickHouse queries this process can have in flight.

    Snuba serves WSGI on granian, where a blocking thread handles one request --
    and so at most one query -- at a time. That makes this a known constant
    rather than a guess, and the right basis for sizing a connection pool: a pool
    larger than the thread count can never be fully checked out.

    ``SNUBA_QUERY_CONCURRENCY`` is for processes granian does not drive
    (consumers, replacer, CLI jobs) that know their own thread count.
    """
    concurrency = _positive_int_env("SNUBA_QUERY_CONCURRENCY") or _positive_int_env(
        "GRANIAN_BLOCKING_THREADS"
    )

    if concurrency is None:
        # Mirror granian's own default rather than guess: it computes
        # blocking_threads as backpressure // 2, and backpressure as
        # backlog // workers.
        backpressure = _positive_int_env("GRANIAN_BACKPRESSURE")
        if backpressure is None:
            backlog = _positive_int_env("GRANIAN_BACKLOG")
            workers = _positive_int_env("GRANIAN_WORKERS")
            if backlog is not None and workers is not None:
                backpressure = backlog // workers
        if backpressure is not None:
            concurrency = max(1, backpressure // 2)

    if concurrency is None:
        # Lazy import: snuba.settings would otherwise cycle back through here.
        from snuba import settings

        # The `snuba api` CLI passes API_THREADS to granian as blocking_threads.
        # It is None unless a settings module sets it.
        concurrency = settings.API_THREADS or _DEFAULT_QUERY_CONCURRENCY

    if concurrency <= 0:
        concurrency = _DEFAULT_QUERY_CONCURRENCY

    return min(concurrency, _MAX_QUERY_CONCURRENCY)
