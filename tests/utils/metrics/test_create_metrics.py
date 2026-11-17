from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from unittest.mock import MagicMock, patch

from snuba.utils.metrics.backends.dualwrite import SentryDatadogMetricsBackend
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.metrics.util import AGGREGATION_FLUSH_INTERVAL_SECONDS, create_metrics

SOCKET_PATH = "unixgram:///var/run/dogstatsd.sock"


@patch("datadog.DogStatsd")
def test_create_metrics_uses_uds(dogstatsd: MagicMock) -> None:
    with patch.multiple(
        "snuba.settings",
        TESTING=False,
        DOGSTATSD_SOCKET_PATH=SOCKET_PATH,
    ):
        backend = create_metrics("snuba.test")
        assert isinstance(backend, SentryDatadogMetricsBackend)
        # Force the lazily-created DogStatsd client to be built.
        backend.increment("snuba.test.metric")

    # The address reaches the client verbatim, scheme included.
    dogstatsd.assert_called_once_with(
        socket_path=SOCKET_PATH,
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
        DOGSTATSD_SOCKET_PATH=SOCKET_PATH,
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
        assert call.kwargs["socket_path"] == SOCKET_PATH


def test_create_metrics_dummy_without_a_socket() -> None:
    with patch.multiple(
        "snuba.settings",
        TESTING=False,
        DOGSTATSD_SOCKET_PATH=None,
    ):
        backend = create_metrics("snuba.test")

    assert isinstance(backend, DummyMetricsBackend)


def _options(values: Mapping[str, object]) -> Callable[[str, object], object]:
    def get_option(key: str, default: object) -> object:
        return values.get(key, default)

    return get_option


@patch("snuba.utils.metrics.util.atexit.register")
@patch("datadog.DogStatsd")
def test_create_metrics_shares_an_aggregating_client_when_enabled(
    dogstatsd: MagicMock, atexit_register: MagicMock
) -> None:
    options = {"dogstatsd_client_aggregation": True, "dogstatsd_max_samples_per_context": 5}
    with (
        patch.multiple(
            "snuba.settings",
            TESTING=False,
            DOGSTATSD_SOCKET_PATH=SOCKET_PATH,
        ),
        patch("snuba.state.sentry_options.get_option", _options(options)),
    ):
        backend = create_metrics("snuba.test")
        backend.increment("snuba.test.metric")

        def emit_from_thread() -> None:
            backend.increment("snuba.test.metric")

        worker = threading.Thread(target=emit_from_thread)
        worker.start()
        worker.join()

    # One client for the whole process, buffered and aggregating.
    dogstatsd.assert_called_once_with(
        socket_path=SOCKET_PATH,
        namespace="snuba.test",
        constant_tags=None,
        disable_telemetry=False,
        disable_buffering=False,
        disable_aggregation=False,
        flush_interval=AGGREGATION_FLUSH_INTERVAL_SECONDS,
        max_metric_samples_per_context=5,
    )
    assert dogstatsd.return_value.increment.call_count == 2
    # The last flush window is sent on exit.
    atexit_register.assert_called_once_with(dogstatsd.return_value.stop)


@patch("snuba.utils.metrics.util.atexit.register")
@patch("datadog.DogStatsd")
def test_create_metrics_keeps_per_thread_clients_when_disabled(
    dogstatsd: MagicMock, atexit_register: MagicMock
) -> None:
    options = {"dogstatsd_client_aggregation": False}
    with (
        patch.multiple(
            "snuba.settings",
            TESTING=False,
            DOGSTATSD_SOCKET_PATH=SOCKET_PATH,
        ),
        patch("snuba.state.sentry_options.get_option", _options(options)),
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
        assert "disable_aggregation" not in call.kwargs
    atexit_register.assert_not_called()
