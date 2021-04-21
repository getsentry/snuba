import click


@click.command()
@click.option("--bootstrap/--no-bootstrap", default=True)
@click.option("--workers/--no-workers", default=True)
def devserver(*, bootstrap: bool, workers: bool) -> None:
    "Starts all Snuba processes for local development."
    import os
    import sys
    from subprocess import list2cmdline, call
    from honcho.manager import Manager
    from snuba import settings

    if settings.ERRORS_ROLLOUT_WRITABLE_STORAGE:
        events_storage = "errors"
    else:
        events_storage = "events"

    os.environ["PYTHONUNBUFFERED"] = "1"

    if bootstrap:
        cmd = ["snuba", "bootstrap", "--force", "--no-migrate"]
        if not workers:
            cmd.append("--no-kafka")
        returncode = call(cmd)
        if returncode > 0:
            sys.exit(returncode)

        # Run migrations
        returncode = call(["snuba", "migrations", "migrate", "--force"])
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
                "--commit-log-topic=snuba-commit-log",
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
            "outcomes-consumer",
            [
                "snuba",
                "consumer",
                "--auto-offset-reset=latest",
                "--log-level=debug",
                "--storage=outcomes_raw",
                "--consumer-group=outcomes_group",
            ],
        ),
        (
            "consumer",
            [
                "snuba",
                "consumer",
                "--auto-offset-reset=latest",
                "--log-level=debug",
                f"--storage={events_storage}",
            ],
        ),
        (
            "replacer",
            [
                "snuba",
                "replacer",
                "--auto-offset-reset=latest",
                "--log-level=debug",
                f"--storage={events_storage}",
            ],
        ),
        (
            "subscriptions-consumer-events",
            [
                "snuba",
                "subscriptions",
                "--auto-offset-reset=latest",
                "--log-level=debug",
                "--max-batch-size=1",
                "--consumer-group=snuba-events-subscriptions-consumers",
                "--topic=events",
                "--result-topic=events-subscription-results",
                "--dataset=events",
                "--commit-log-topic=snuba-commit-log",
                "--commit-log-group=snuba-consumers",
                "--delay-seconds=1",
                "--schedule-ttl=10",
                "--max-query-workers=1",
            ],
        ),
        (
            "subscriptions-consumer-transactions",
            [
                "snuba",
                "subscriptions",
                "--auto-offset-reset=latest",
                "--log-level=debug",
                "--max-batch-size=1",
                "--consumer-group=snuba-transactions-subscriptions-consumers",
                "--topic=events",
                "--result-topic=transactions-subscription-results",
                "--dataset=transactions",
                "--commit-log-topic=snuba-commit-log",
                "--commit-log-group=transactions_group",
                "--delay-seconds=1",
                "--schedule-ttl=10",
                "--max-query-workers=1",
            ],
        ),
        (
            "cdc-consumer",
            [
                "snuba",
                "multistorage-consumer",
                "--auto-offset-reset=latest",
                "--log-level=debug",
                "--storage=groupedmessages",
                "--storage=groupassignees",
                "--consumer-group=cdc_group",
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
