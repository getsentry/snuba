from __future__ import annotations

import multiprocessing

from granian import Granian
from granian.constants import Interfaces


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
    # Granian starts workers with the global multiprocessing start method. Use
    # spawn so workers don't inherit locks or half-imported modules from
    # threads (e.g. the Sentry SDK's) that are running in this process when
    # the workers start.
    multiprocessing.set_start_method("spawn", force=True)

    host, port = bind.rsplit(":", maxsplit=1)
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
