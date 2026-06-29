from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from snuba.utils.metrics.backends.dualwrite import SentryDatadogMetricsBackend
from snuba.utils.metrics.util import create_metrics


def _runtime_config(use_uds: str) -> Any:
    """Return a ``state.get_config`` replacement that only overrides the
    ``use_dogstatsd_uds`` flag and falls back to the provided default otherwise."""

    def get_config(key: str, default: Any = None, *args: Any, **kwargs: Any) -> Any:
        if key == "use_dogstatsd_uds":
            return use_uds
        return default

    return get_config


@patch("datadog.DogStatsd")
def test_create_metrics_uses_uds_when_flag_enabled(dogstatsd: MagicMock) -> None:
    with (
        patch.multiple(
            "snuba.settings",
            TESTING=False,
            DOGSTATSD_HOST="localhost",
            DOGSTATSD_PORT=8125,
            DOGSTATSD_SOCKET_PATH="/var/run/dogstatsd.sock",
        ),
        patch("snuba.state.get_config", side_effect=_runtime_config("1")),
    ):
        backend = create_metrics("snuba.test")
        assert isinstance(backend, SentryDatadogMetricsBackend)
        # Force the lazily-created DogStatsd client to be built.
        backend.increment("snuba.test.metric")

    dogstatsd.assert_called_once_with(
        socket_path="/var/run/dogstatsd.sock",
        namespace="snuba.test",
        constant_tags=None,
    )


@patch("datadog.DogStatsd")
def test_create_metrics_uses_udp_when_flag_disabled(dogstatsd: MagicMock) -> None:
    with (
        patch.multiple(
            "snuba.settings",
            TESTING=False,
            DOGSTATSD_HOST="localhost",
            DOGSTATSD_PORT=8125,
            # A socket path alone must not force UDS: the runtime flag gates it.
            DOGSTATSD_SOCKET_PATH="/var/run/dogstatsd.sock",
        ),
        patch("snuba.state.get_config", side_effect=_runtime_config("0")),
    ):
        backend = create_metrics("snuba.test")
        assert isinstance(backend, SentryDatadogMetricsBackend)
        backend.increment("snuba.test.metric")

    dogstatsd.assert_called_once_with(
        host="localhost",
        port=8125,
        namespace="snuba.test",
        constant_tags=None,
    )


def test_create_metrics_socket_only_without_host_port_is_dummy() -> None:
    # UDS is gated by the flag and uses host/port as the UDP transport, so a deployment
    # with only a socket and no host/port has no UDP target -> DummyMetricsBackend, even
    # with the flag on. This keeps the flag authoritative and matches the Rust
    # select_transport behavior. The decision is static (no snuba.state lookup).
    with (
        patch.multiple(
            "snuba.settings",
            TESTING=False,
            DOGSTATSD_HOST=None,
            DOGSTATSD_PORT=None,
            DOGSTATSD_SOCKET_PATH="/var/run/dogstatsd.sock",
        ),
        patch("snuba.state.get_config") as get_config,
    ):
        backend = create_metrics("snuba.test")
        get_config.assert_not_called()

    from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

    assert isinstance(backend, DummyMetricsBackend)


def test_create_metrics_dummy_does_not_import_state() -> None:
    # With nothing configured, create_metrics() must return the dummy backend without
    # consulting snuba.state. create_metrics() runs at snuba.environment import time,
    # and importing snuba.state there is a circular import, so the no-metrics path
    # must not reach the runtime-config lookup.
    with (
        patch.multiple(
            "snuba.settings",
            TESTING=False,
            DOGSTATSD_HOST=None,
            DOGSTATSD_PORT=None,
            DOGSTATSD_SOCKET_PATH=None,
        ),
        patch("snuba.state.get_config") as get_config,
    ):
        backend = create_metrics("snuba.test")
        get_config.assert_not_called()

    from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

    assert isinstance(backend, DummyMetricsBackend)
