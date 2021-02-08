import click
from pkgutil import walk_packages


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
`OoO'  o   O `OoO'o `OoO' `OoO'o
"""


for loader, module_name, is_pkg in walk_packages(__path__, __name__ + "."):  # type: ignore  # mypy issue #1422
    module = __import__(module_name, globals(), locals(), ["__name__"])
    cmd = getattr(module, module_name.rsplit(".", 1)[-1])
    if isinstance(cmd, click.Command):
        main.add_command(cmd)
