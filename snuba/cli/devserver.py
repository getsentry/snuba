import click

from snuba import settings


@click.command()
def devserver():
    "Starts all Snuba processes for local development."
    import sys
    from subprocess import list2cmdline, check_output
    from honcho.manager import Manager

    check_output(['snuba', 'bootstrap', '--force'])

    daemons = [
        ('api', ['snuba', 'api']),
        ('consumer', ['snuba', 'consumer', '--auto-offset-reset=latest', '--log-level=debug']),
        ('replacer', ['snuba', 'replacer', '--auto-offset-reset=latest', '--log-level=debug']),
    ]

    manager = Manager()
    for name, cmd in daemons:
        manager.add_process(
            name,
            list2cmdline(cmd),
            quiet=False,
        )

    manager.loop()
    sys.exit(manager.returncode)
