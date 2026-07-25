from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

from snuba.utils.metrics.backends.dualwrite import SentryDatadogMetricsBackend
from snuba.utils.metrics.util import create_metrics


@patch("datadog.DogStatsd")
def test_create_metrics_uses_uds(dogstatsd: MagicMock) -> None:
    with patch.multiple(
        "snuba.settings",
        TESTING=False,
        DOGSTATSD_SOCKET_PATH="unixgram:///var/run/dogstatsd.sock",
    ):
        backend = create_metrics("snuba.test")
        assert isinstance(backend, SentryDatadogMetricsBackend)
        # Force the lazily-created DogStatsd client to be built.
        backend.increment("snuba.test.metric")

    # The socket address is passed verbatim, scheme included: the datadog client strips the
    # scheme and picks the socket kind itself, the same way the Rust exporter parses it.
    dogstatsd.assert_called_once_with(
        socket_path="unixgram:///var/run/dogstatsd.sock",
        namespace="snuba.test",
        constant_tags=None,
        disable_telemetry=False,
    )


@patch("datadog.DogStatsd")
def test_create_metrics_builds_a_uds_client_per_thread(dogstatsd: MagicMock) -> None:
    # DatadogMetricsBackend builds a DogStatsd client per thread, so the client factory
    # runs on each thread's first emission. Every thread must get a socket client.
    with patch.multiple(
        "snuba.settings",
        TESTING=False,
        DOGSTATSD_SOCKET_PATH="unixgram:///var/run/dogstatsd.sock",
    ):
        backend = create_metrics("snuba.test")
        backend.increment("snuba.test.metric")

        def emit_from_thread() -> None:
            backend.increment("snuba.test.metric")

        worker = threading.Thread(target=emit_from_thread)
        worker.start()
        worker.join()

    assert dogstatsd.call_count == 2
    for call in dogstatsd.call_args_list:
        assert call.kwargs["socket_path"] == "unixgram:///var/run/dogstatsd.sock"


def test_create_metrics_dummy_without_a_socket() -> None:
    # With no socket configured, create_metrics() returns the dummy backend.
    with patch.multiple(
        "snuba.settings",
        TESTING=False,
        DOGSTATSD_SOCKET_PATH=None,
    ):
        backend = create_metrics("snuba.test")

    from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

    assert isinstance(backend, DummyMetricsBackend)
