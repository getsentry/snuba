from __future__ import annotations

from granian import Granian
from granian.constants import Interfaces

from snuba.utils.concurrency import declare_query_concurrency, granian_blocking_threads


def resolve_blocking_threads(threads: int | None, backlog: int, processes: int) -> int:
    """The number of WSGI blocking threads granian will actually run.

    Computed rather than left to granian so the same number reaches both granian
    and the ClickHouse pool sizing: with ``API_THREADS`` unset, ``snuba api``
    leaves ``blocking_threads=None`` and granian derives 64 while the pools fall
    back to 8.
    """
    return granian_blocking_threads(threads, backlog, processes)


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
    # Hand granian the same number the pools size from, so the two cannot
    # drift. The granian CLI sets GRANIAN_* itself and never comes through here.
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
