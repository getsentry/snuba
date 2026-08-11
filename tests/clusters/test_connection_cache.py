"""The ConnectionCache owns driver façades and the HTTP client/socket manager.

Kept out of test_cluster.py because none of this needs a live ClickHouse, and
that module's autouse fixture reloads the cluster module between tests.
"""

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


def test_connection_cache_owns_the_client_manager() -> None:
    # One owner for façades plus the HTTP client/socket manager. Each façade is
    # handed the cache's manager rather than reaching for a global of its own.
    cache = ConnectionCache()
    with override_options("snuba", {"use_clickhouse_connect_driver": True}):
        pool = _get(cache, ClickhouseClientSettings.QUERY)
        again = _get(cache, ClickhouseClientSettings.TRACING)

    # Different profiles, different façades -- but one manager, which is what
    # lets them share a client when their timeouts match.
    assert pool is not again
    manager = pool._ClickhouseConnectPool__client_manager  # type: ignore[attr-defined]
    assert again._ClickhouseConnectPool__client_manager is manager  # type: ignore[attr-defined]


def test_native_path_builds_no_client_manager() -> None:
    # The manager is built on first use of the connect driver, so a native-only
    # process neither imports clickhouse-connect nor allocates a socket pool.
    cache = ConnectionCache()
    with override_options("snuba", {"use_clickhouse_connect_driver": False}):
        _get(cache, ClickhouseClientSettings.QUERY)

    assert cache._ConnectionCache__client_manager is None  # type: ignore[attr-defined]


def test_fork_drops_pools_and_resets_clients() -> None:
    # Façades and clients reference the parent's sockets. The child must not use
    # them, and must not close them either -- the descriptors are shared.
    # Dropping the references is enough; the child rebuilds lazily.
    cache = ConnectionCache()
    inherited_pool = mock.Mock()
    cache._ConnectionCache__cache["key"] = inherited_pool  # type: ignore[attr-defined]
    manager = mock.Mock()
    cache._ConnectionCache__client_manager = manager  # type: ignore[attr-defined]
    held = cache._ConnectionCache__lock  # type: ignore[attr-defined]

    cache.reset_after_fork()

    assert cache._ConnectionCache__cache == {}  # type: ignore[attr-defined]
    manager.reset_after_fork.assert_called_once()
    inherited_pool.close.assert_not_called()  # the parent is still using it
    # A lock held at fork time is inherited held, so it is rebuilt, not taken.
    assert cache._ConnectionCache__lock is not held  # type: ignore[attr-defined]


def test_close_drops_pools_and_closes_clients() -> None:
    cache = ConnectionCache()
    cache._ConnectionCache__cache["key"] = mock.Mock()  # type: ignore[attr-defined]
    manager = mock.Mock()
    cache._ConnectionCache__client_manager = manager  # type: ignore[attr-defined]

    cache.close()

    assert cache._ConnectionCache__cache == {}  # type: ignore[attr-defined]
    manager.close.assert_called_once()


def test_fork_handler_is_registered_on_the_cache() -> None:
    # Asserts the effect, not a call count: test_cluster.py reloads the cluster
    # module between tests, so several handlers end up registered by the time the
    # full suite gets here. They are idempotent; what matters is a clear child.
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
