import logging
import time
from pkgutil import walk_packages

import click
import sentry_sdk
import structlog

from snuba.environment import metrics as environment_metrics
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.wrapper import MetricsWrapper

setup_sentry()
setup_logging("DEBUG")
logger = logging.getLogger("snuba_init")

start = time.perf_counter()
logger.info("Initializing Snuba...")
metrics = MetricsWrapper(environment_metrics, "cli")


@click.group()
@click.version_option()
def main() -> None:
    """\b
                         o
                        O
                        O
                        o
    .oOo  'OoOo. O   o  OoOo. .oOoO'
    `Ooo.  o   O o   O  O   o O   o
        O  O   o O   o  o   O o   O
    `OoO'  o   O `OoO'o `OoO' `OoO'o"""


with sentry_sdk.start_transaction(
    op="snuba_init", name="Snuba CLI Initialization", sampled=True
):
    for loader, module_name, is_pkg in walk_packages(__path__, __name__ + "."):
        logger.info(f"Loading module {module_name}")
        with sentry_sdk.start_span(op="import", description=module_name):
            module = __import__(module_name, globals(), locals(), ["__name__"])
            cmd = getattr(module, module_name.rsplit(".", 1)[-1])
            if isinstance(cmd, click.Command):
                main.add_command(cmd)

init_time = time.perf_counter() - start
metrics.gauge("snuba_init", init_time)
logger.info(f"Snuba initialization took {init_time}s")

structlog.reset_defaults()
