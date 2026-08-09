from collections.abc import Generator, Mapping

import pytest

from snuba.utils import concurrency
from snuba.utils.concurrency import process_query_concurrency

GRANIAN_ENV = (
    "SNUBA_QUERY_CONCURRENCY",
    "GRANIAN_BLOCKING_THREADS",
    "GRANIAN_BACKPRESSURE",
    "GRANIAN_BACKLOG",
    "GRANIAN_WORKERS",
)


@pytest.fixture(autouse=True)
def clear_env_and_cache(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
    # Clear both sides so a warm cache cannot leak in, nor this env leak out.
    for name in GRANIAN_ENV:
        monkeypatch.delenv(name, raising=False)
    process_query_concurrency.cache_clear()
    yield
    process_query_concurrency.cache_clear()


def set_env(monkeypatch: pytest.MonkeyPatch, env: Mapping[str, str]) -> None:
    # No cache_clear() needed: the autouse fixture cleared it and no test reads
    # the value before calling this.
    for key, value in env.items():
        monkeypatch.setenv(key, value)


@pytest.mark.parametrize(
    "env, expected",
    [
        # What production sets.
        pytest.param({"GRANIAN_BLOCKING_THREADS": "8"}, 8, id="blocking_threads"),
        # Non-granian processes state their own concurrency.
        pytest.param(
            {"SNUBA_QUERY_CONCURRENCY": "4", "GRANIAN_BLOCKING_THREADS": "8"},
            4,
            id="explicit override wins",
        ),
        # granian's own default when blocking_threads is unset.
        pytest.param({"GRANIAN_BACKPRESSURE": "24"}, 12, id="backpressure // 2"),
        pytest.param(
            {"GRANIAN_BACKLOG": "192", "GRANIAN_WORKERS": "4"},
            24,
            id="backlog // workers // 2",
        ),
        # blocking_threads beats the derived backpressure default.
        pytest.param(
            {"GRANIAN_BLOCKING_THREADS": "8", "GRANIAN_BACKPRESSURE": "24"},
            8,
            id="blocking_threads beats backpressure",
        ),
    ],
)
def test_resolution_order(
    monkeypatch: pytest.MonkeyPatch, env: Mapping[str, str], expected: int
) -> None:
    set_env(monkeypatch, env)
    assert process_query_concurrency() == expected


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("not-a-number", id="non-numeric"),
        pytest.param("0", id="zero"),
        pytest.param("-1", id="negative"),
        pytest.param("", id="empty"),
    ],
)
def test_unusable_values_fall_back_to_the_default(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # A malformed value must not crash startup.
    set_env(monkeypatch, {"GRANIAN_BLOCKING_THREADS": value})
    assert process_query_concurrency() == concurrency._DEFAULT_QUERY_CONCURRENCY


def test_value_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    # Pool size derives from this, so a bogus value must not go unbounded.
    set_env(monkeypatch, {"GRANIAN_BLOCKING_THREADS": "100000"})
    assert process_query_concurrency() == concurrency._MAX_QUERY_CONCURRENCY


def test_falls_back_to_api_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    # The `snuba api` CLI path passes API_THREADS to granian as blocking_threads.
    from snuba import settings

    monkeypatch.setattr(settings, "API_THREADS", 6, raising=False)
    process_query_concurrency.cache_clear()
    assert process_query_concurrency() == 6


def test_defaults_when_nothing_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    from snuba import settings

    monkeypatch.setattr(settings, "API_THREADS", None, raising=False)
    process_query_concurrency.cache_clear()
    assert process_query_concurrency() == concurrency._DEFAULT_QUERY_CONCURRENCY


def test_api_threads_unset_falls_back_without_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    # API_THREADS is None by default; comparing it to an int would raise
    # TypeError on every unconfigured process.
    from snuba import settings

    monkeypatch.setattr(settings, "API_THREADS", None, raising=False)
    process_query_concurrency.cache_clear()
    assert process_query_concurrency() == concurrency._DEFAULT_QUERY_CONCURRENCY


def test_non_positive_api_threads_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    from snuba import settings

    monkeypatch.setattr(settings, "API_THREADS", 0, raising=False)
    process_query_concurrency.cache_clear()
    assert process_query_concurrency() == concurrency._DEFAULT_QUERY_CONCURRENCY


def test_declare_query_concurrency_is_read_back(monkeypatch: pytest.MonkeyPatch) -> None:
    concurrency.declare_query_concurrency(32)
    assert process_query_concurrency() == 32


def test_declare_query_concurrency_does_not_override_explicit_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_env(monkeypatch, {"SNUBA_QUERY_CONCURRENCY": "4"})
    concurrency.declare_query_concurrency(32)
    assert process_query_concurrency() == 4


def test_declare_query_concurrency_invalidates_a_warm_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Without the cache_clear this is silently order-dependent, which is the
    # class of bug the declaration exists to prevent.
    assert process_query_concurrency() == concurrency._DEFAULT_QUERY_CONCURRENCY
    concurrency.declare_query_concurrency(32)
    assert process_query_concurrency() == 32


@pytest.mark.parametrize(
    "threads, backlog, processes",
    [
        pytest.param(None, 128, 1, id="snuba admin defaults"),
        pytest.param(None, 128, 1, id="snuba api, 1 process"),
        pytest.param(None, 256, 4, id="snuba api, 4 processes"),
        pytest.param(None, 1024, 3, id="uneven division"),
        pytest.param(None, 64, 1, id="below granian's backlog floor"),
        pytest.param(8, 128, 1, id="explicit threads"),
        pytest.param(1, 128, 8, id="explicit threads, many processes"),
    ],
)
def test_resolve_blocking_threads_matches_granian(
    threads: int | None, backlog: int, processes: int
) -> None:
    # The whole point of resolving the thread count ourselves is to hand granian
    # and the ClickHouse pools the same number, so pin our arithmetic against
    # what granian actually computes rather than against a copy of the formula.
    from granian import Granian
    from granian.constants import Interfaces

    from snuba.utils.server import resolve_blocking_threads

    granian_server = Granian(
        target="snuba.web.wsgi:application",
        interface=Interfaces.WSGI,
        backlog=backlog,
        workers=processes,
        blocking_threads=threads,
    )

    assert resolve_blocking_threads(threads, backlog, processes) == granian_server.blocking_threads


def test_serve_gives_granian_and_the_pools_the_same_thread_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The regression: `snuba api` with API_THREADS unset left blocking_threads
    # as None, granian derived 64 from the backlog, and the pools -- seeing no
    # GRANIAN_* env var -- sized to 8.
    from unittest import mock

    from snuba.utils import server

    with mock.patch.object(server, "Granian") as granian:
        server.serve("snuba.web.wsgi:application", "127.0.0.1:1218", processes=1, backlog=128)

    _, kwargs = granian.call_args
    assert kwargs["blocking_threads"] == 64
    assert process_query_concurrency() == 64
