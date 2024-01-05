import click

from snuba import settings

COMMON_CONSUMER_DEV_OPTIONS = [
    "--auto-offset-reset=latest",
    "--no-strict-offset-reset",
    "--log-level=debug",
    "--use-rust-processor",
    "--enforce-schema",
]


@click.command()
@click.option("--bootstrap/--no-bootstrap", default=True)
@click.option("--workers/--no-workers", default=True)
def devserver(*, bootstrap: bool, workers: bool) -> None:
    "Starts all Snuba processes for local development."
    import os
    import sys
    from subprocess import call, list2cmdline

    from honcho.manager import Manager

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
        (("admin", ["snuba", "admin"])),
        (
            "transaction-consumer",
            [
                "snuba",
                "rust-consumer",
                "--storage=transactions",
                "--consumer-group=transactions_group",
                *COMMON_CONSUMER_DEV_OPTIONS,
            ],
        ),
        (
            "outcomes-consumer",
            [
                "snuba",
                "rust-consumer",
                "--storage=outcomes_raw",
                "--consumer-group=outcomes_group",
                *COMMON_CONSUMER_DEV_OPTIONS,
            ],
        ),
        (
            "errors-consumer",
            [
                "snuba",
                "rust-consumer",
                "--storage=errors",
                *COMMON_CONSUMER_DEV_OPTIONS,
            ],
        ),
        (
            "replacer",
            [
                "snuba",
                "replacer",
                "--auto-offset-reset=latest",
                "--no-strict-offset-reset",
                "--log-level=debug",
                "--storage=errors",
            ],
        ),
        (
            "spans-consumer",
            [
                "snuba",
                "rust-consumer",
                "--storage=spans",
                "--consumer-group=spans_group",
                "--use-rust-processor",
                *COMMON_CONSUMER_DEV_OPTIONS,
            ],
        ),
    ]

    if settings.SEPARATE_SCHEDULER_EXECUTOR_SUBSCRIPTIONS_DEV:
        daemons += [
            (
                "subscriptions-scheduler-events",
                [
                    "snuba",
                    "subscriptions-scheduler",
                    "--entity=events",
                    "--consumer-group=snuba-events-subscriptions-scheduler",
                    "--followed-consumer-group=snuba-consumers",
                    "--auto-offset-reset=latest",
                    "--log-level=debug",
                    "--schedule-ttl=10",
                ],
            ),
            (
                "subscriptions-executor-events",
                [
                    "snuba",
                    "subscriptions-executor",
                    "--dataset=events",
                    "--entity=events",
                    "--consumer-group=snuba-events-subscription-executor",
                    "--auto-offset-reset=latest",
                ],
            ),
            (
                "subscriptions-scheduler-transactions",
                [
                    "snuba",
                    "subscriptions-scheduler",
                    "--entity=transactions",
                    "--consumer-group=snuba-transactions-subscriptions-scheduler",
                    "--followed-consumer-group=transactions_group",
                    "--auto-offset-reset=latest",
                    "--log-level=debug",
                    "--schedule-ttl=10",
                ],
            ),
            (
                "subscriptions-executor-transactions",
                [
                    "snuba",
                    "subscriptions-executor",
                    "--dataset=transactions",
                    "--entity=transactions",
                    "--consumer-group=snuba-transactions-subscription-executor",
                    "--auto-offset-reset=latest",
                ],
            ),
        ]

    else:
        daemons += [
            (
                "subscriptions-scheduler-executor-events",
                [
                    "snuba",
                    "subscriptions-scheduler-executor",
                    "--dataset=events",
                    "--entity=events",
                    "--consumer-group=snuba-events-subscriptions-scheduler-executor",
                    "--followed-consumer-group=snuba-consumers",
                    "--auto-offset-reset=latest",
                    "--no-strict-offset-reset",
                    "--log-level=debug",
                    "--schedule-ttl=10",
                    "--stale-threshold-seconds=900",
                ],
            ),
            (
                "subscriptions-scheduler-executor-transactions",
                [
                    "snuba",
                    "subscriptions-scheduler-executor",
                    "--dataset=transactions",
                    "--entity=transactions",
                    "--consumer-group=snuba-transactions-subscriptions-scheduler-executor",
                    "--followed-consumer-group=transactions_group",
                    "--auto-offset-reset=latest",
                    "--no-strict-offset-reset",
                    "--log-level=debug",
                    "--schedule-ttl=10",
                    "--stale-threshold-seconds=900",
                ],
            ),
        ]

    if settings.ENABLE_SENTRY_METRICS_DEV:
        daemons += [
            (
                "metrics-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=metrics_raw",
                    "--consumer-group=snuba-metrics-consumers",
                    *COMMON_CONSUMER_DEV_OPTIONS,
                ],
            ),
            (
                "generic-metrics-distributions-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=generic_metrics_distributions_raw",
                    "--consumer-group=snuba-gen-metrics-distributions-consumers",
                    *COMMON_CONSUMER_DEV_OPTIONS,
                ],
            ),
            (
                "generic-metrics-sets-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=generic_metrics_sets_raw",
                    "--consumer-group=snuba-gen-metrics-sets-consumers",
                    *COMMON_CONSUMER_DEV_OPTIONS,
                ],
            ),
            (
                "generic-metrics-counters-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=generic_metrics_counters_raw",
                    "--consumer-group=snuba-gen-metrics-counters-consumers",
                    *COMMON_CONSUMER_DEV_OPTIONS,
                ],
            ),
            (
                "generic-metrics-gauges-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=generic_metrics_gauges_raw",
                    "--consumer-group=snuba-gen-metrics-gauges-consumers",
                    *COMMON_CONSUMER_DEV_OPTIONS,
                ],
            ),
        ]
        if settings.ENABLE_METRICS_SUBSCRIPTIONS:
            if settings.SEPARATE_SCHEDULER_EXECUTOR_SUBSCRIPTIONS_DEV:
                daemons += [
                    (
                        "subscriptions-scheduler-metrics-counters",
                        [
                            "snuba",
                            "subscriptions-scheduler",
                            "--entity=metrics_counters",
                            "--consumer-group=snuba-metrics-subscriptions-scheduler",
                            "--followed-consumer-group=snuba-metrics-consumers",
                            "--auto-offset-reset=latest",
                            "--log-level=debug",
                            "--schedule-ttl=10",
                        ],
                    ),
                    (
                        "subscriptions-scheduler-metrics-sets",
                        [
                            "snuba",
                            "subscriptions-scheduler",
                            "--entity=metrics_sets",
                            "--consumer-group=snuba-metrics-subscriptions-scheduler",
                            "--followed-consumer-group=snuba-metrics-consumers",
                            "--auto-offset-reset=latest",
                            "--log-level=debug",
                            "--schedule-ttl=10",
                        ],
                    ),
                    (
                        "subscriptions-scheduler-generic-metrics-distributions",
                        [
                            "snuba",
                            "subscriptions-scheduler",
                            "--entity=generic_metrics_distributions",
                            "--consumer-group=snuba-generic-metrics-distributions-subscriptions-schedulers",
                            "--followed-consumer-group=snuba-generic-metrics-distributions-consumers",
                            "--auto-offset-reset=latest",
                            "--log-level=debug",
                            "--schedule-ttl=10",
                        ],
                    ),
                    (
                        "subscriptions-scheduler-generic-metrics-sets",
                        [
                            "snuba",
                            "subscriptions-scheduler",
                            "--entity=generic_metrics_sets",
                            "--consumer-group=snuba-generic-metrics-sets-subscriptions-schedulers",
                            "--followed-consumer-group=snuba-generic-metrics-sets-consumers",
                            "--auto-offset-reset=latest",
                            "--log-level=debug",
                            "--schedule-ttl=10",
                        ],
                    ),
                    (
                        "subscriptions-scheduler-generic-metrics-counters",
                        [
                            "snuba",
                            "subscriptions-scheduler",
                            "--entity=generic_metrics_counters",
                            "--consumer-group=snuba-generic-metrics-counters-subscriptions-schedulers",
                            "--followed-consumer-group=snuba-generic-metrics-counters-consumers",
                            "--auto-offset-reset=latest",
                            "--log-level=debug",
                            "--schedule-ttl=10",
                        ],
                    ),
                    (
                        "subscriptions-executor-metrics",
                        [
                            "snuba",
                            "subscriptions-executor",
                            "--dataset=metrics",
                            "--entity=metrics_counters",
                            "--entity=metrics_sets",
                            "--consumer-group=snuba-metrics-subscription-executor",
                            "--auto-offset-reset=latest",
                        ],
                    ),
                ]
            else:
                daemons += [
                    (
                        "subscriptions-scheduler-executor-metrics",
                        [
                            "snuba",
                            "subscriptions-scheduler-executor",
                            "--dataset=metrics",
                            "--entity=metrics_sets",
                            "--entity=metrics_counters",
                            "--consumer-group=snuba-metrics-subscriptions-scheduler-executor",
                            "--followed-consumer-group=snuba-metrics-consumers",
                            "--auto-offset-reset=latest",
                            "--no-strict-offset-reset",
                            "--log-level=debug",
                            "--schedule-ttl=10",
                        ],
                    ),
                ]

    if settings.ENABLE_PROFILES_CONSUMER:
        daemons += [
            (
                "profiles",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=profiles",
                    *COMMON_CONSUMER_DEV_OPTIONS,
                ],
            ),
            (
                "functions",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=functions_raw",
                    *COMMON_CONSUMER_DEV_OPTIONS,
                ],
            ),
        ]

    if settings.ENABLE_REPLAYS_CONSUMER:
        daemons += [
            (
                "replays-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=replays",
                    *COMMON_CONSUMER_DEV_OPTIONS,
                ],
            ),
        ]

    if settings.ENABLE_ISSUE_OCCURRENCE_CONSUMER:
        daemons += [
            (
                "issue-occurrence-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=search_issues",
                    "--consumer-group=generic_events_group",
                    "--use-rust-processor",
                    *COMMON_CONSUMER_DEV_OPTIONS,
                ],
            ),
        ]

    if settings.ENABLE_GROUP_ATTRIBUTES_CONSUMER:
        daemons += [
            (
                "group-attributes-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=group_attributes",
                    "--consumer-group=group_attributes_group",
                    *COMMON_CONSUMER_DEV_OPTIONS,
                ],
            ),
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
