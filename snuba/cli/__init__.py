import click
import logging
from pkgutil import walk_packages

from snuba import settings


@click.group()
@click.version_option()
@click.option('--log-level', default=settings.LOG_LEVEL, help='Logging level to use.')
def main(*, log_level: str):
    """\b
                     o
                    O
                    O
                    o
.oOo  'OoOo. O   o  OoOo. .oOoO'
`Ooo.  o   O o   O  O   o O   o
    O  O   o O   o  o   O o   O
`OoO'  o   O `OoO'o `OoO' `OoO'o
"""
    logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s %(levelname)-8s %(message)s')


for loader, module_name, is_pkg in walk_packages(__path__, __name__ + '.'):
    module = __import__(module_name, globals(), locals(), ['__name__'])
    cmd = getattr(module, module_name.rsplit('.', 1)[-1])
    if isinstance(cmd, click.Command):
        main.add_command(cmd)
