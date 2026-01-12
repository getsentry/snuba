from __future__ import annotations

from typing import Optional

from granian import Granian
from granian.constants import Interfaces


def serve(
    module: str,
    bind: str,
    processes: int = 1,
    threads: Optional[int] = None,
    backlog: int = 128,
    reload: bool = False,
    name: str | None = None,
    lifetime: int | None = None,
) -> None:
    host, port = bind.rsplit(":", maxsplit=1)
    server = Granian(
        target=module,
        address=host,
        port=int(port),
        interface=Interfaces.WSGI,
        backlog=backlog,
        workers=processes,
        workers_lifetime=lifetime,
        workers_kill_timeout=30,
        blocking_threads=threads,
        respawn_failed_workers=True,
        reload=reload,
        process_name=name,
    )
    server.serve()  # type: ignore
