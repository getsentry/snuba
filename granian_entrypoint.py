#!/usr/bin/env python3
"""
Python replacement for granian_entrypoint.sh — compatible with distroless images.
"""

import os
import time
import urllib.error
import urllib.request


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
    os.execvp(args[0], args)


if __name__ == "__main__":
    main()
