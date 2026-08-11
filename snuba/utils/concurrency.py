from __future__ import annotations

import os
from functools import lru_cache

_DEFAULT_QUERY_CONCURRENCY = 8

# granian's own default and floor for the backlog (granian/cli.py and
# granian/server/common.py). Mirrored rather than imported: granian does not
# export them, and reading the env vars is how we learn what the CLI was given.
_GRANIAN_DEFAULT_BACKLOG = 1024
_GRANIAN_MIN_BACKLOG = 128

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


def granian_blocking_threads(
    threads: int | None,
    backlog: int = _GRANIAN_DEFAULT_BACKLOG,
    workers: int = 1,
    backpressure: int | None = None,
) -> int:
    """The number of WSGI blocking threads granian will actually run.

    Mirrors ``granian/server/common.py``: the backlog has a floor of 128,
    backpressure defaults to ``backlog // workers``, and a WSGI worker runs
    ``backpressure // 2`` blocking threads. The defaults here are granian's own,
    so a caller that knows only some of the values gets what granian would do
    with the rest.
    """
    if threads is not None:
        return max(1, threads)
    backpressure = max(1, backpressure or max(_GRANIAN_MIN_BACKLOG, backlog) // max(1, workers))
    return max(1, backpressure // 2)


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
    # The value is cached on first read, so drop it here.
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
        backlog = _positive_int_env("GRANIAN_BACKLOG")
        workers = _positive_int_env("GRANIAN_WORKERS")
        backpressure = _positive_int_env("GRANIAN_BACKPRESSURE")
        # Any one of them means granian is driving, and it defaults the rest --
        # so requiring the full set would size to _DEFAULT_QUERY_CONCURRENCY
        # against a granian running many times that.
        if backlog is not None or workers is not None or backpressure is not None:
            concurrency = granian_blocking_threads(
                None,
                backlog if backlog is not None else _GRANIAN_DEFAULT_BACKLOG,
                workers if workers is not None else 1,
                backpressure,
            )

    if concurrency is None:
        # Lazy import: snuba.settings would otherwise cycle back through here.
        from snuba import settings

        # `snuba api` passes API_THREADS to granian as blocking_threads.
        concurrency = settings.API_THREADS or _DEFAULT_QUERY_CONCURRENCY

    if concurrency <= 0:
        concurrency = _DEFAULT_QUERY_CONCURRENCY

    return min(concurrency, _MAX_QUERY_CONCURRENCY)
