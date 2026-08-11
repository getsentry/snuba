"""ConnectionCache: driver pool cache and fork reset."""

import os
from unittest import mock

from sentry_options.testing import override_options

from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseNode,
    ConnectionCache,
)


def _node() -> ClickhouseNode:
    return ClickhouseNode("host", 9000, http_port=8123)


def _get(cache: ConnectionCache, profile: ClickhouseClientSettings) -> object:
    return cache.get_node_connection(
        profile,
        _node(),
        "u",
        "p",
        "db",
        secure=False,
        ca_certs=None,
        verify=False,
    )


def test_caches_pools_per_profile() -> None:
    cache = ConnectionCache()
    with override_options("snuba", {"use_clickhouse_connect_driver": True}):
        a = _get(cache, ClickhouseClientSettings.QUERY)
        b = _get(cache, ClickhouseClientSettings.QUERY)
        c = _get(cache, ClickhouseClientSettings.TRACING)
    assert a is b
    assert a is not c


def test_fork_clears_cache() -> None:
    from snuba.clusters.cluster import connection_cache

    cache = connection_cache._ConnectionCache__cache  # type: ignore[attr-defined]
    cache["inherited"] = mock.Mock()
    try:
        pid = os.fork()
        if pid == 0:
            inherited = connection_cache._ConnectionCache__cache  # type: ignore[attr-defined]
            os._exit(0 if inherited == {} else 1)
        _, status = os.waitpid(pid, 0)
    finally:
        cache.pop("inherited", None)
    assert os.WEXITSTATUS(status) == 0
