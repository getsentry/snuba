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


@patch("datadog.DogStatsd")
def test_create_metrics_uses_uds_without_host_or_port(dogstatsd: MagicMock) -> None:
    # A UDS-only deployment (no UDP host/port configured) must still emit metrics
    # over the socket, not silently fall back to the no-op DummyMetricsBackend.
    with (
        patch.multiple(
            "snuba.settings",
            TESTING=False,
            DOGSTATSD_HOST=None,
            DOGSTATSD_PORT=None,
            DOGSTATSD_SOCKET_PATH="/var/run/dogstatsd.sock",
        ),
        patch("snuba.state.get_config", side_effect=_runtime_config("1")),
    ):
        backend = create_metrics("snuba.test")
        assert isinstance(backend, SentryDatadogMetricsBackend)
        backend.increment("snuba.test.metric")

    dogstatsd.assert_called_once_with(
        socket_path="/var/run/dogstatsd.sock",
        namespace="snuba.test",
        constant_tags=None,
    )
