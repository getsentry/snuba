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
