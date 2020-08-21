import click


@click.command()
@click.option("--bootstrap/--no-bootstrap", default=True)
@click.option("--workers/--no-workers", default=True)
@click.option("--migrate/--no-migrate", default=True)
def devserver(*, bootstrap: bool, workers: bool, migrate: bool) -> None:
    "Starts all Snuba processes for local development."
    import os
    import sys
    from subprocess import list2cmdline, call
    from honcho.manager import Manager

    os.environ["PYTHONUNBUFFERED"] = "1"

    if bootstrap:
        cmd = ["snuba", "bootstrap", "--force"]
        if not workers:
            cmd.append("--no-kafka")
        if not migrate:
            cmd.append("--no-migrate")
        returncode = call(cmd)
        if returncode > 0:
            sys.exit(returncode)

    daemons = [("api", ["snuba", "api"])]

    if not workers:
        os.execvp(daemons[0][1][0], daemons[0][1])

    daemons += [
        (
            "transaction-consumer",
            [
                "snuba",
                "consumer",
                "--auto-offset-reset=latest",
                "--log-level=debug",
                "--storage=transactions",
                "--consumer-group=transactions_group",
            ],
        ),
        (
            "sessions-consumer",
            [
                "snuba",
                "consumer",
                "--auto-offset-reset=latest",
                "--log-level=debug",
                "--storage=sessions_raw",
                "--consumer-group=sessions_group",
            ],
        ),
        (
            "consumer",
            [
                "snuba",
                "consumer",
                "--auto-offset-reset=latest",
                "--log-level=debug",
                "--storage=events",
            ],
        ),
        (
            "replacer",
            [
                "snuba",
                "replacer",
                "--auto-offset-reset=latest",
                "--log-level=debug",
                "--storage=events",
            ],
        ),
    ]

    manager = Manager()
    for name, cmd in daemons:
        manager.add_process(
            name, list2cmdline(cmd), quiet=False,
        )

    manager.loop()
    sys.exit(manager.returncode)
