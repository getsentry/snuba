#!/usr/bin/env python
"""Print the DogStatsD payloads snuba emits, for local debugging.

Binds a Unix datagram socket and dumps everything written to it. Point snuba at it with:

    SNUBA_DOGSTATSD_SOCKET_PATH='unixgram:///tmp/snuba-dogstatsd.sock'

The socket path defaults to /tmp/snuba-dogstatsd.sock and can be overridden with a
positional argument. Note that the argument here is a plain filesystem path, not the
scheme-prefixed address that SNUBA_DOGSTATSD_SOCKET_PATH takes.
"""

from __future__ import annotations

import contextlib
import os
import socket
import sys

DEFAULT_PATH = "/tmp/snuba-dogstatsd.sock"


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH

    # A stale socket file from a previous run would make bind() fail with EADDRINUSE.
    with contextlib.suppress(FileNotFoundError):
        os.unlink(path)

    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
        sock.bind(path)
        print(f"listening on unixgram://{path}", file=sys.stderr)
        try:
            while True:
                # DogStatsD payloads are newline-delimited and already fit one datagram.
                sys.stdout.write(sock.recv(65536).decode("utf-8", errors="replace"))
                sys.stdout.flush()
        except KeyboardInterrupt:
            pass
        finally:
            os.unlink(path)


if __name__ == "__main__":
    main()
