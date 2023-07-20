import os

import pytest
from click.testing import CliRunner

from snuba.cli.optimize import optimize


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_optimize_cli() -> None:
    runner = CliRunner()
    host = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
    port = os.environ.get("CLICKHOUSE_PORT", "9000")
    result = runner.invoke(
        optimize,
        [
            "--clickhouse-host",
            host,
            "--clickhouse-port",
            port,
            "--parallel",
            "2",
            "--storage",
            "errors",
        ],
    )
    assert result.exit_code == 0, result.output
