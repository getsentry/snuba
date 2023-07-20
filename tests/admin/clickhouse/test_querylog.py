from __future__ import annotations

from typing import Type

import pytest

from snuba import state
from snuba.admin.clickhouse.querylog import (
    _MAX_CH_THREADS,
    BadThreadsValue,
    _get_clickhouse_threads,
)


@pytest.mark.parametrize(
    "config_val, expected_threads",
    [
        pytest.param(2, 2, id="normal"),
        pytest.param(5, _MAX_CH_THREADS, id="over max"),
    ],
)
@pytest.mark.redis_db
def test_get_clickhouse_threads(config_val: str | int, expected_threads: int) -> None:
    state.set_config("admin.querylog_threads", str(config_val))
    assert _get_clickhouse_threads() == expected_threads


@pytest.mark.parametrize(
    "config_val, error",
    [
        pytest.param("invalid_value", BadThreadsValue, id="invalid_value"),
    ],
)
@pytest.mark.redis_db
def test_get_clickhouse_threads_error(
    config_val: str | int, error: Type[Exception]
) -> None:
    state.set_config("admin.querylog_threads", str(config_val))
    with pytest.raises(error):
        _get_clickhouse_threads()
