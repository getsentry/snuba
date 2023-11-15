from typing import Sequence
from unittest.mock import Mock, patch

from click.testing import CliRunner

import snuba.cli.consumer
import snuba.consumers.consumer_builder
from snuba.cli.consumer import consumer


# mock builder
@patch.object(snuba.cli.consumer, "ConsumerBuilder")
@patch.object(snuba.cli.consumer, "check_clickhouse_connections")
# mock signal handlers so other tests don't get affected
@patch.object(snuba.cli.consumer, "signal")
# don't set the metrics global state
@patch.object(snuba.cli.consumer, "configure_metrics")
def test_consumer_cli(*_mocks: Sequence[Mock]) -> None:
    runner = CliRunner()
    result = runner.invoke(
        consumer,
        [
            "--storage",
            "querylog",
            "--auto-offset-reset",
            "latest",
            "--health-check-file",
            "/tmp/test",
            "--group-instance-id",
            "test_group_instance_id",
        ],
    )
    assert result.exit_code == 0, result.output
