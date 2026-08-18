import sys
from collections.abc import Sequence
from unittest.mock import Mock, patch

from click.testing import CliRunner

from snuba.cli.rust_consumer import rust_consumer


@patch("snuba.cli.rust_consumer.asdict", return_value={})
@patch("snuba.cli.rust_consumer.resolve_consumer_config")
def test_rust_consumer_passes_group_instance_id(
    resolve_config: Mock, *_mocks: Sequence[Mock]
) -> None:
    """ops static_membership deployments pass --group-instance-id to rust-consumer."""
    resolve_config.return_value = Mock()
    mock_rust = Mock()
    mock_rust.consumer.return_value = 0

    runner = CliRunner()
    with patch.dict(sys.modules, {"rust_snuba": mock_rust}):
        result = runner.invoke(
            rust_consumer,
            [
                "--storage",
                "eap_items",
                "--consumer-group",
                "snuba-eap-items-consumers",
                "--group-instance-id",
                "snuba-eap-items-consumer-0",
            ],
        )

    assert result.exit_code == 0, result.output
    resolve_config.assert_called_once()
    assert resolve_config.call_args.kwargs["group_instance_id"] == "snuba-eap-items-consumer-0"
    mock_rust.consumer.assert_called_once()
