from typing import Sequence
from unittest.mock import Mock, patch

from click.testing import CliRunner

import snuba.cli.replacer
import snuba.replacer
import snuba.utils.streams.configuration_builder
from snuba.cli.replacer import replacer


@patch("arroyo.processing.StreamProcessor")
@patch("arroyo.backends.kafka.KafkaConsumer")
@patch(
    "snuba.utils.streams.configuration_builder.build_kafka_consumer_configuration",
    return_value={"bootstrap.servers": "localhost"},
)
@patch.object(snuba.replacer, "ReplacerStrategyFactory")
@patch.object(snuba.replacer, "ReplacerWorker")
@patch.object(snuba.cli.replacer, "get_writable_storage")
@patch.object(snuba.cli.replacer, "setup_logging")
@patch.object(snuba.cli.replacer, "setup_sentry")
@patch.object(snuba.cli.replacer, "configure_metrics")
@patch.object(snuba.cli.replacer, "signal")
def test_replacer_cli(
    *_mocks: Sequence[Mock],
) -> None:
    storage = Mock()
    topic_spec = Mock()
    topic_spec.topic_name = "replacements-topic"
    topic_spec.topic = Mock()
    storage.get_table_writer.return_value.get_stream_loader.return_value.get_replacement_topic_spec.return_value = (
        topic_spec
    )
    snuba.cli.replacer.get_writable_storage.return_value = storage

    worker = Mock()
    snuba.replacer.ReplacerWorker.return_value = worker

    runner = CliRunner()
    result = runner.invoke(
        replacer,
        [
            "--storage",
            "errors",
            "--health-check-file",
            "/tmp/health.txt",
            "--max-poll-interval-ms",
            "12345",
        ],
    )

    assert result.exit_code == 0, result.output
    assert snuba.replacer.ReplacerStrategyFactory.call_args.kwargs == {
        "worker": worker,
        "health_check_file": "/tmp/health.txt",
    }
    assert snuba.utils.streams.configuration_builder.build_kafka_consumer_configuration.call_args.kwargs[
        "override_params"
    ] == {
        "max.poll.interval.ms": 12345,
        "session.timeout.ms": 12345,
    }
