#!/usr/bin/env python3
"""
Python equivalent of granian_entrypoint.sh.

Mirrors the behavior of the bash script currently shipped to SaaS:
optionally waits for envoy admin to report LIVE, then execs granian
with the WSGI app.

Provides a shell-free entrypoint so deployment manifests can invoke
`python3 ./granian_entrypoint.py` instead of `./granian_entrypoint.sh`.
"""

import os
import time
import urllib.error
import urllib.request
from datetime import datetime


def wait_for_envoy() -> None:
    port = os.environ.get("ENVOY_ADMIN_PORT")
    if not port:
        print("ENVOY_ADMIN_PORT env var is unset")
        print("Fall through to snuba start without check")
        return

    url = f"http://localhost:{port}/ready"
    print(f"Check envoy readiness on {url}")
    while True:
        time.sleep(1)
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.read().decode().strip() == "LIVE":
                    print("Envoy is ready")
                    return
        except Exception:
            pass
        print("Envoy not ready, looping..")


def main() -> None:
    wait_for_envoy()

    wsgi_target = os.environ.get("SNUBA_WSGI_TARGET", "snuba.web.wsgi:application")
    args = ["granian", "--interface", "wsgi", "--host", "0.0.0.0", "--http", "auto", wsgi_target]

    if os.environ.get("ENABLE_HEAPTRACK"):
        # The bash predecessor had a latent bug: it built a `heaptrack …` argv
        # via `set --` but then `exec granian …` instead of `exec "$@"`, so the
        # wrapping never took effect. Fix it here by actually prepending.
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args = ["heaptrack", "-o", f"./profiler_data/profile_{timestamp}"] + args

    os.execvp(args[0], args)


if __name__ == "__main__":
    main()
