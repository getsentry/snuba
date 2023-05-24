from __future__ import annotations

import logging
import os
import time
from typing import Any

import click
import sentry_sdk
import structlog

from snuba.core import initialize
from snuba.environment import metrics as environment_metrics
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.wrapper import MetricsWrapper

setup_sentry()


setup_logging("DEBUG")
logger = logging.getLogger("snuba_init")

start = time.perf_counter()
metrics = MetricsWrapper(environment_metrics, "cli")

plugin_folder = os.path.dirname(__file__)


class SnubaCLI(click.MultiCommand):
    def list_commands(self, ctx: Any) -> list[str]:
        rv = []
        for filename in os.listdir(plugin_folder):
            if filename.endswith(".py") and filename != "__init__.py":
                # IMPORTANT!!!!
                # Click replaces underscores with dashes for command names
                # by default. To mimic this behavior we have to do that here
                # since all of our infrastructure depends on this being the
                # case
                rv.append(filename[:-3].replace("_", "-"))
        rv.sort()
        return rv

    def get_command(self, ctx: Any, name: str) -> click.Command:
        # IMPORTANT!!!!
        # Click replaces underscores with dashes for command names
        # by default. To mimic this behavior we have to do that here
        # since all of our infrastructure depends on this being the
        # case
        with sentry_sdk.start_transaction(
            op="snuba_init", name=f"[cli init] {name}", sampled=True
        ):
            actual_command_name = name.replace("-", "_")
            ns: dict[str, click.Command] = {}
            # NOTE: WE initialize snuba before we compile the command code
            # That way if any command code references any snuba construct that needs
            # to be initialized (e.g. a factory) at import time, it is already initialized
            # into the runtime
            initialize.initialize_snuba()
            fn = os.path.join(plugin_folder, actual_command_name + ".py")
            with open(fn) as f:
                code = compile(f.read(), fn, "exec")
                eval(code, ns, ns)
            init_time = time.perf_counter() - start
            metrics.timing("snuba_init_time", init_time)
            logger.info(f"Snuba initialization took {init_time}s")
        return ns[actual_command_name]


@click.command(cls=SnubaCLI)
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


structlog.reset_defaults()
setup_logging()
