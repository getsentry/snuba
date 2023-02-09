import pytest

from snuba import state
from snuba.admin.clickhouse.querylog import _MAX_CH_THREADS, _get_clickhouse_threads


@pytest.mark.parametrize(
    "config_val, expected_threads",
    [
        pytest.param(2, 2, id="normal"),
        pytest.param(5, _MAX_CH_THREADS, id="over max"),
        pytest.param("invalid_value", _MAX_CH_THREADS, id="invalid_value"),
    ],
)
def test_get_clickhouse_threads(config_val, expected_threads) -> None:
    state.set_config("admin.querylog_threads", config_val)
    assert _get_clickhouse_threads() == expected_threads
