import os

import click

from snuba.datasets.storages.factory import get_writable_storage_keys
from snuba.settings.utils import write_settings_to_json

RUST_ENVIRONMENT = os.environ.get("RUST_ENVIRONMENT", "debug")
RUST_PATH = f"rust_snuba/target/{RUST_ENVIRONMENT}/consumer"


@click.command()
@click.option(
    "--storage",
    "storage_name",
    type=click.Choice(
        [storage_key.value for storage_key in get_writable_storage_keys()]
    ),
    help="The storage to target",
    required=True,
)
@click.option(
    "--log-level",
    "log_level",
    type=click.Choice(["error", "warn", "info", "debug", "trace"]),
    help="Logging level to use.",
    default="info",
)
def rust_consumer(*, storage_name: str, log_level: str) -> None:
    """
    Experimental alternative to`snuba consumer`
    """
    settings_path = write_settings_to_json()

    os.execve(
        RUST_PATH,
        ["--", "--storage", storage_name, "--settings-path", settings_path],
        {"RUST_LOG": log_level},
    )
