import os
from typing import Sequence

import click

from snuba.datasets.storages.factory import get_writable_storage_keys
from snuba.settings.utils import write_settings_to_json

RUST_ENVIRONMENT = os.environ.get("RUST_ENVIRONMENT", "debug")
RUST_PATH = f"rust_snuba/target/{RUST_ENVIRONMENT}/consumer"


@click.command()
@click.option(
    "--storage",
    "storage_names",
    type=click.Choice(
        [storage_key.value for storage_key in get_writable_storage_keys()]
    ),
    help="The storage to target",
    multiple=True,
    required=True,
)
@click.option(
    "--log-level",
    "log_level",
    type=click.Choice(["error", "warn", "info", "debug", "trace"]),
    help="Logging level to use.",
    default="info",
)
def rust_consumer(*, storage_names: Sequence[str], log_level: str) -> None:
    """
    Experimental alternative to`snuba consumer`
    """
    # TODO: compile minimal consumer config here, and pass it down to rust
    # code, instead of having rust code read the storage yaml. Incomplete list
    # of things that are unimplemented:
    #
    # - slices
    # - multi-storage consumer
    # - consumer group (?)
    # - topic cli arg
    # - interaction between BROKER_CONFIG + KAFKA_TOPIC_MAP +
    # KAFKA_BROKER_CONFIG had to be reimplemented. But does it match the Python
    # impl?
    #
    # ideally the config passed to rust would contain physical topic name, the
    # right kafka broker config and clickhouse cluster to use, and nothing
    # else.
    #
    # there is an argument for keeping it as-is, and that is if we need to read
    # the storage yaml at compile-time, to do code generation. It might be
    # necessary if we want to define import paths for Rust message processors
    # in yaml, e.g.:
    #
    # stream_loader:
    #   processor:
    #     python_name: snuba.processors.QuerylogProcessor

    settings_path = write_settings_to_json()

    rust_consumer_args = ["--", "--settings-path", settings_path]

    for storage_name in storage_names:
        rust_consumer_args.extend(["--storage", storage_name])

    os.execve(
        RUST_PATH,
        rust_consumer_args,
        {"RUST_LOG": log_level},
    )
