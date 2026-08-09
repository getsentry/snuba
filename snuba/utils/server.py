from __future__ import annotations

from granian import Granian
from granian.constants import Interfaces

from snuba.utils.concurrency import declare_query_concurrency


def resolve_blocking_threads(threads: int | None, backlog: int, processes: int) -> int:
    """The number of WSGI blocking threads granian will actually run.

    Mirrors granian's own derivation (``granian/server/common.py``): the backlog
    has a floor of 128, backpressure defaults to ``backlog // workers``, and a
    WSGI worker runs ``backpressure // 2`` blocking threads.

    We compute it rather than let granian do it so the value can be handed to
    both granian and the ClickHouse pool sizing, which otherwise disagree: with
    ``API_THREADS`` unset, ``snuba api`` leaves ``blocking_threads=None`` and
    granian derives 64, while the pools fall back to 8.
    """
    if threads is not None:
        return max(1, threads)
    backpressure = max(1, max(128, backlog) // max(1, processes))
    return max(1, backpressure // 2)


def serve(
    module: str,
    bind: str,
    processes: int = 1,
    threads: int | None = None,
    backlog: int = 128,
    reload: bool = False,
    name: str | None = None,
    lifetime: int | None = None,
    max_rss: int | None = None,
) -> None:
    host, port = bind.rsplit(":", maxsplit=1)
    # Resolve the thread count here and hand granian the same number the pools
    # size from, so the two cannot drift. Deployments that run the granian CLI
    # directly set GRANIAN_* themselves and never come through here.
    threads = resolve_blocking_threads(threads, backlog, processes)
    declare_query_concurrency(threads)
    server = Granian(
        target=module,
        address=host,
        port=int(port),
        interface=Interfaces.WSGI,
        backlog=backlog,
        workers=processes,
        workers_lifetime=lifetime,
        workers_max_rss=max_rss,
        workers_kill_timeout=30,
        blocking_threads=threads,
        respawn_failed_workers=True,
        reload=reload,
        process_name=name,
    )
    server.serve()
