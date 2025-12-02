from __future__ import annotations

from typing import NoReturn

from granian import Granian
from granian.constants import Interfaces


def serve(
    module: str,
    bind: str,
    processes: int = 1,
    threads: int = 1,
    reload: bool = False,
    name: str | None = None,
) -> NoReturn:
    host, port = bind.rsplit(":", maxsplit=1)
    server = Granian(
        target=module,
        address=host,
        port=port,
        interface=Interfaces.WSGI,
        workers=processes,
        workers_kill_timeout=30,
        blocking_threads=threads,
        respawn_failed_workers=True,
        reload=reload,
        process_name=name,
    )
    server.serve()
