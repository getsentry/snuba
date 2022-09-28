import logging
import time
from pkgutil import walk_packages

import click

logging.basicConfig(level=getattr(logging, "INFO"), format="%(asctime)s %(message)s")
logger = logging.getLogger("snuba_startup")

start = time.perf_counter()
logger.info("Initializing Snuba...")


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


for loader, module_name, is_pkg in walk_packages(__path__, __name__ + "."):
    logger.info(f"Loading module {module_name}")
    module = __import__(module_name, globals(), locals(), ["__name__"])
    cmd = getattr(module, module_name.rsplit(".", 1)[-1])
    if isinstance(cmd, click.Command):
        main.add_command(cmd)

logger.info(f"Snuba initialization took {time.perf_counter() - start}s")
