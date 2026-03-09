import os
from typing import Optional


def get_statsd_addr() -> tuple[Optional[str], Optional[int]]:
    """Returns the address of the StatsD server."""
    snuba_statsd_address = os.environ.get("SNUBA_STATSD_ADDR")
    if snuba_statsd_address:
        ip, separator, port = snuba_statsd_address.rpartition(":")
        assert separator == ":", "SNUBA_STATSD_ADDR must be in the format of ip:port"
        return ip, int(port or 8125)

    dogstatsd_host = os.environ.get("DOGSTATSD_HOST")
    dogstatsd_port = os.environ.get("DOGSTATSD_PORT", "8125")
    if dogstatsd_host and dogstatsd_port:
        return dogstatsd_host, int(dogstatsd_port)

    return None, None
