import logging
import os
import subprocess
from typing import Optional

import click
import sentry_sdk

from snuba.environment import setup_logging, setup_sentry

logger = logging.getLogger(__name__)
RUST_ENVIRONMENT = os.environ.get("RUST_ENVIRONMENT", "debug")
RUST_PATH = f"rust_snuba/target/{RUST_ENVIRONMENT}/examples"


@click.command()
@click.option("--log-level", help="Logging level to use.")
@click.option(
    "--storage",
    "storage_name",
    type=click.Choice(["querylog"]),
    help="The storage to target",
    required=True,
)
def rust_consumer(
    *,
    storage_name: str,
    log_level: Optional[str] = None,
) -> None:
    """
    (Experimental) Runs the rust consumer implementation for a storage
    """
    setup_logging(log_level)
    setup_sentry()
    logger.info("Consumer Starting")
    # storage_key = StorageKey(storage_name)
    sentry_sdk.set_tag("storage", storage_name)

    subprocess.run(
        [f"{RUST_PATH}/{storage_name}_consumer"],
        check=True,
        # stdout=subprocess.STDOUT,
        # stderr=subprocess.PIPE,
    )
