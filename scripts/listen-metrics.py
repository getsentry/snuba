#!/usr/bin/env python3
"""Print the DogStatsD payloads snuba emits, for local debugging.

Binds a Unix datagram socket and dumps everything written to it.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import socket
import sys

DEFAULT_PATH = "/tmp/snuba-dogstatsd.sock"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path",
        nargs="?",
        default=DEFAULT_PATH,
        help=(
            "Filesystem path to bind, without a transport scheme. Point snuba at it with "
            "SNUBA_DOGSTATSD_SOCKET_PATH='unixgram://<path>'. Default: %(default)s"
        ),
    )
    path = parser.parse_args().path

    # A stale socket file from a previous run would make bind() fail with EADDRINUSE.
    with contextlib.suppress(FileNotFoundError):
        os.unlink(path)

    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
        sock.bind(path)
        print(f"listening on unixgram://{path}", file=sys.stderr)
        with contextlib.suppress(KeyboardInterrupt):
            while True:
                # DogStatsD payloads are newline-delimited and already fit one datagram.
                sys.stdout.write(sock.recv(65536).decode("utf-8", errors="replace"))
                sys.stdout.flush()


if __name__ == "__main__":
    main()
