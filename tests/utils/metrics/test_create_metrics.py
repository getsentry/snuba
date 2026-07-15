from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from snuba.utils.metrics.backends.dualwrite import SentryDatadogMetricsBackend
from snuba.utils.metrics.util import create_metrics


def _get_option(use_uds: bool) -> Any:
    """Return a ``sentry_options.get_option`` replacement that only overrides the
    ``use_dogstatsd_uds`` flag and falls back to the provided default otherwise."""

    def get_option(key: str, default: Any) -> Any:
        if key == "use_dogstatsd_uds":
            return use_uds
        return default

    return get_option


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
        patch("snuba.state.sentry_options.get_option", side_effect=_get_option(True)),
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
            # A socket path alone must not force UDS: the sentry-option gates it.
            DOGSTATSD_SOCKET_PATH="/var/run/dogstatsd.sock",
        ),
        patch("snuba.state.sentry_options.get_option", side_effect=_get_option(False)),
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
def test_create_metrics_transport_decision_is_process_wide(dogstatsd: MagicMock) -> None:
    # DatadogMetricsBackend builds a DogStatsd client per thread, so the client factory
    # runs on each thread's first emission. The use_dogstatsd_uds decision must be
    # resolved once per process so a mid-flight flip cannot put one thread on UDP and
    # another on UDS. Emit once with the flag off (-> UDP), flip it on, then emit from a
    # fresh thread and assert that thread still builds a UDP client (never a socket one).
    import threading

    with patch.multiple(
        "snuba.settings",
        TESTING=False,
        DOGSTATSD_HOST="localhost",
        DOGSTATSD_PORT=8125,
        DOGSTATSD_SOCKET_PATH="/var/run/dogstatsd.sock",
    ):
        with patch("snuba.state.sentry_options.get_option", side_effect=_get_option(False)):
            backend = create_metrics("snuba.test")
            backend.increment("snuba.test.metric")  # main thread resolves -> UDP

        with patch("snuba.state.sentry_options.get_option", side_effect=_get_option(True)):

            def emit_from_thread() -> None:
                backend.increment("snuba.test.metric")

            worker = threading.Thread(target=emit_from_thread)
            worker.start()
            worker.join()

    # One client built per thread, and every one uses UDP (host/port), not the socket,
    # even though the flag flipped to True before the worker thread emitted.
    assert dogstatsd.call_count == 2
    for call in dogstatsd.call_args_list:
        assert "socket_path" not in call.kwargs
        assert call.kwargs["host"] == "localhost"
        assert call.kwargs["port"] == 8125


def test_create_metrics_socket_only_without_host_port_is_dummy() -> None:
    # UDS is gated by the flag and uses host/port as the UDP transport, so a deployment
    # with only a socket and no host/port has no UDP target -> DummyMetricsBackend, even
    # with the flag on. This keeps the flag authoritative and matches the Rust
    # select_transport behavior. The decision is static (no sentry-options lookup).
    with (
        patch.multiple(
            "snuba.settings",
            TESTING=False,
            DOGSTATSD_HOST=None,
            DOGSTATSD_PORT=None,
            DOGSTATSD_SOCKET_PATH="/var/run/dogstatsd.sock",
        ),
        patch("snuba.state.sentry_options.get_option") as get_option,
    ):
        backend = create_metrics("snuba.test")
        get_option.assert_not_called()

    from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

    assert isinstance(backend, DummyMetricsBackend)


def test_create_metrics_dummy_does_not_read_options() -> None:
    # With nothing configured, create_metrics() must return the dummy backend without
    # consulting sentry-options. create_metrics() runs at snuba.environment import time,
    # and importing snuba.state.sentry_options there is a circular import, so the
    # no-metrics path must not reach the option lookup.
    with (
        patch.multiple(
            "snuba.settings",
            TESTING=False,
            DOGSTATSD_HOST=None,
            DOGSTATSD_PORT=None,
            DOGSTATSD_SOCKET_PATH=None,
        ),
        patch("snuba.state.sentry_options.get_option") as get_option,
    ):
        backend = create_metrics("snuba.test")
        get_option.assert_not_called()

    from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

    assert isinstance(backend, DummyMetricsBackend)
