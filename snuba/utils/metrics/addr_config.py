import os
from typing import Optional


def get_statsd_addr() -> tuple[Optional[str], Optional[int]]:
    """Returns the address of the StatsD server."""
    SNUBA_STATSD_ADDR = os.environ.get("SNUBA_STATSD_ADDR")
    if SNUBA_STATSD_ADDR:
        ip, separator, port = SNUBA_STATSD_ADDR.rpartition(":")
        assert separator == ":", "SNUBA_STATSD_ADDR must be in the format of ip:port"
        return ip, int(port or 8125)

    DOGSTATSD_HOST = os.environ.get("DOGSTATSD_HOST")
    DOGSTATSD_PORT = os.environ.get("DOGSTATSD_PORT")
    if DOGSTATSD_HOST and DOGSTATSD_PORT:
        return DOGSTATSD_HOST, int(DOGSTATSD_PORT or 8125)

    return None, None
