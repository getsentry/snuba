from __future__ import annotations

import logging
import os
import time

import click
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

plugin_folder = os.path.dirname(__file__)


class SnubaCLI(click.MultiCommand):
    def list_commands(self, ctx) -> list[str]:
        rv = []
        for filename in os.listdir(plugin_folder):
            if filename.endswith(".py") and filename != "__init__.py":
                rv.append(filename[:-3])
        rv.sort()
        return rv

    def get_command(self, ctx, name):
        ns = {}
        fn = os.path.join(plugin_folder, name + ".py")
        with open(fn) as f:
            code = compile(f.read(), fn, "exec")
            eval(code, ns, ns)
        return ns[name]


@click.command(cls=SnubaCLI)
def main():
    """snuba"""


# with sentry_sdk.start_transaction(
#     op="snuba_init", name="Snuba CLI Initialization", sampled=True
# ):
# for loader, module_name, is_pkg in walk_packages(__path__, __name__ + "."):
# 	  logger.info(f"Loading module {module_name}")
# 	  with sentry_sdk.start_span(op="import", description=module_name):
# 		  import pdb

# 		  pdb.set_trace()

# 		  module = __import__(module_name, globals(), locals(), ["__name__"])
# 		  cmd = getattr(module, module_name.rsplit(".", 1)[-1])
# 		  if isinstance(cmd, click.Command):
# 			  main.add_command(cmd)

init_time = time.perf_counter() - start
metrics.gauge("snuba_init", init_time)
logger.info(f"Snuba initialization took {init_time}s")

structlog.reset_defaults()
