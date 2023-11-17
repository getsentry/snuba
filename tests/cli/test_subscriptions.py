from typing import Sequence
from unittest.mock import Mock, patch

from click import Command
from click.testing import CliRunner

import snuba.subscriptions.executor_consumer
from snuba.cli.subscriptions_executor import subscriptions_executor
from snuba.cli.subscriptions_scheduler import subscriptions_scheduler
from snuba.cli.subscriptions_scheduler_executor import subscriptions_scheduler_executor


# don't access clickhouse
@patch.object(snuba.cli.subscriptions_executor, "check_clickhouse_connections")
@patch.object(
    snuba.cli.subscriptions_scheduler_executor, "check_clickhouse_connections"
)
@patch.object(snuba.cli.subscriptions_scheduler, "check_clickhouse_connections")
# don't set the metrics global state
@patch.object(snuba.cli.subscriptions_executor, "configure_metrics")
@patch.object(snuba.cli.subscriptions_scheduler_executor, "configure_metrics")
@patch.object(snuba.cli.subscriptions_scheduler, "configure_metrics")
# mock the builders, they are tested elsewhere
@patch.object(snuba.cli.subscriptions_executor, "build_executor_consumer")
@patch.object(
    snuba.cli.subscriptions_scheduler_executor, "build_scheduler_executor_consumer"
)
@patch.object(snuba.cli.subscriptions_scheduler, "SchedulerBuilder")
# mock signal handlers so other tests don't get affected
@patch.object(snuba.cli.subscriptions_executor, "signal")
@patch.object(snuba.cli.subscriptions_scheduler_executor, "signal")
@patch.object(snuba.cli.subscriptions_scheduler, "signal")
def test_subscriptions_cli(*_mocks: Sequence[Mock]) -> None:
    runner = CliRunner()
    _test_cli(
        runner,
        subscriptions_executor,
        [
            "--dataset",
            "events",
            "--entity",
            "events",
            "--health-check-file",
            "/tmp/test",
        ],
    )

    _test_cli(
        runner,
        subscriptions_scheduler_executor,
        [
            "--dataset",
            "events",
            "--entity",
            "events",
            "--consumer-group",
            "test",
            "--followed-consumer-group",
            "test",
            "--health-check-file",
            "/tmp/test",
        ],
    )

    _test_cli(
        runner,
        subscriptions_scheduler,
        [
            "--entity",
            "events",
            "--followed-consumer-group",
            "test",
            "--health-check-file",
            "/tmp/test",
        ],
    )


def _test_cli(runner: CliRunner, command: Command, args: Sequence[str]) -> None:
    result = runner.invoke(command, args)
    assert result.exit_code == 0, result.output
