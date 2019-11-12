import click


@click.command()
@click.option('--bootstrap/--no-bootstrap', default=True)
@click.option('--workers/--no-workers', default=True)
def devserver(bootstrap, workers):
    "Starts all Snuba processes for local development."
    import logging
    import os
    import sys
    from subprocess import list2cmdline, call
    from honcho.manager import Manager

    log_level = logging.getLevelName(logging.getLogger().getEffectiveLevel())
    command = ['snuba', f'--log-level={log_level}']

    os.environ['PYTHONUNBUFFERED'] = '1'

    if bootstrap:
        cmd = command + ['bootstrap', '--force']
        if not workers:
            cmd.append('--no-kafka')
        returncode = call(cmd)
        if returncode > 0:
            sys.exit(returncode)

    daemons = [
        ('api', [
            'uwsgi', '--master', '--manage-script-name',
            '--wsgi-file', 'snuba/views.py',
            '--http', '0.0.0.0:1218',
            '--http-keepalive', '--need-app', '--die-on-term',
        ]),
    ]

    if not workers:
        os.execvp(daemons[0][1][0], daemons[0][1])

    daemons += [
        ('transaction-consumer', command + ['consumer', '--auto-offset-reset=latest', '--dataset=transactions', '--consumer-group=transactions_group']),
        ('consumer', command + ['consumer', '--auto-offset-reset=latest']),
        ('replacer', command + ['replacer', '--auto-offset-reset=latest']),
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
