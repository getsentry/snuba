from __future__ import annotations

import pytest
from sentry_options.testing import override_options

from snuba.admin.clickhouse.querylog import _MAX_CH_THREADS, _get_clickhouse_threads


@pytest.mark.parametrize(
    "config_val, expected_threads",
    [
        pytest.param(2, 2, id="normal"),
        pytest.param(5, _MAX_CH_THREADS, id="over max"),
    ],
)
@pytest.mark.redis_db
def test_get_clickhouse_threads(config_val: int, expected_threads: int) -> None:
    with override_options("snuba", {"admin.querylog_threads": config_val}):
        assert _get_clickhouse_threads() == expected_threads
