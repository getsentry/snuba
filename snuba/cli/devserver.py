import click

from snuba import settings


@click.command()
def devserver():
    "Starts all Snuba processes for local development."
    import sys
    import time
    from subprocess import list2cmdline, check_output
    from honcho.manager import Manager

    from snuba.clickhouse import ClickhousePool

    attempts = 0
    while True:
        try:
            ClickhousePool().execute('SELECT 1')
            break
        except Exception:
            attempts += 1
            if attempts == 10:
                raise
            time.sleep(1)

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
