import click


@click.command()
@click.option('--bootstrap/--no-bootstrap', default=True)
def devserver(bootstrap):
    "Starts all Snuba processes for local development."
    import os
    import sys
    import time
    from subprocess import list2cmdline, call
    from honcho.manager import Manager

    os.environ['PYTHONUNBUFFERED'] = '1'

    if bootstrap:
        returncode = call(['snuba', 'bootstrap', '--force'])
        if returncode > 0:
            sys.exit(returncode)

    daemons = [
        ('api', ['uwsgi', '--master', '--manage-script-name', '--wsgi-file', 'snuba/api.py', '--http', '0.0.0.0:1218', '--http-keepalive']),
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
