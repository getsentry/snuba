import os
import sys
from subprocess import call, list2cmdline

import click
from honcho.manager import Manager

from snuba import settings

COMMON_RUST_CONSUMER_DEV_OPTIONS = [
    "--use-rust-processor",
    "--auto-offset-reset=latest",
    "--no-strict-offset-reset",
    "--enforce-schema",
]


@click.command()
@click.option("--bootstrap/--no-bootstrap", default=True)
@click.option("--workers/--no-workers", default=True)
@click.option(
    "--log-level", default="info", help="Logging level to use for all processes"
)
def devserver(*, bootstrap: bool, workers: bool, log_level: str) -> None:
    "Starts all Snuba processes for local development."

    os.environ["PYTHONUNBUFFERED"] = "1"

    if bootstrap:
        cmd = ["snuba", "bootstrap", "--force", "--no-migrate"]
        returncode = call(cmd)
        if returncode > 0 and workers:
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
                *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                f"--log-level={log_level}",
            ],
        ),
        (
            "outcomes-consumer",
            [
                "snuba",
                "rust-consumer",
                "--storage=outcomes_raw",
                "--consumer-group=outcomes_group",
                *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                f"--log-level={log_level}",
            ],
        ),
        (
            "outcomes-billing-consumer",
            [
                "snuba",
                "rust-consumer",
                "--storage=outcomes_raw",
                "--consumer-group=outcomes_billing_group",
                "--raw-events-topic=outcomes-billing",
                *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                f"--log-level={log_level}",
            ],
        ),
        (
            "errors-consumer",
            [
                "snuba",
                "rust-consumer",
                "--storage=errors",
                "--consumer-group=errors_group",
                *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                f"--log-level={log_level}",
            ],
        ),
        (
            "replacer",
            [
                "snuba",
                "replacer",
                "--auto-offset-reset=latest",
                "--no-strict-offset-reset",
                f"--log-level={log_level}",
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
                *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                f"--log-level={log_level}",
            ],
        ),
        (
            "eap-items-consumer",
            [
                "snuba",
                "rust-consumer",
                "--storage=eap_items",
                "--consumer-group=eap_items_group",
                "--use-rust-processor",
                *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                f"--log-level={log_level}",
            ],
        ),
        (
            "eap-items-span-consumer",
            [
                "snuba",
                "rust-consumer",
                "--storage=eap_items_span",
                "--consumer-group=eap_items_span_group",
                "--use-rust-processor",
                *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                f"--log-level={log_level}",
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
                    "--followed-consumer-group=errors_group",
                    "--auto-offset-reset=latest",
                    f"--log-level={log_level}",
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
                    f"--log-level={log_level}",
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
                    f"--log-level={log_level}",
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
                    f"--log-level={log_level}",
                ],
            ),
            (
                "subscriptions-scheduler-eap-spans",
                [
                    "snuba",
                    "subscriptions-scheduler",
                    "--entity=eap_items_span",
                    "--consumer-group=snuba-eap_spans-subscriptions-scheduler",
                    "--followed-consumer-group=eap_items_span_group",
                    "--auto-offset-reset=latest",
                    f"--log-level={log_level}",
                    "--schedule-ttl=10",
                ],
            ),
            (
                "subscriptions-executor-eap-spans",
                [
                    "snuba",
                    "subscriptions-executor",
                    "--dataset=events_analytics_platform",
                    "--entity=eap_items_span",
                    "--consumer-group=snuba-eap_spans-subscription-executor",
                    "--auto-offset-reset=latest",
                    f"--log-level={log_level}",
                ],
            ),
            (
                "subscriptions-scheduler-eap-items",
                [
                    "snuba",
                    "subscriptions-scheduler",
                    "--entity=eap_items",
                    "--consumer-group=snuba-eap_items-subscriptions-scheduler",
                    "--followed-consumer-group=eap_items_group",
                    "--auto-offset-reset=latest",
                    f"--log-level={log_level}",
                    "--schedule-ttl=10",
                ],
            ),
            (
                "subscriptions-executor-eap-items",
                [
                    "snuba",
                    "subscriptions-executor",
                    "--dataset=events_analytics_platform",
                    "--entity=eap_items_span",
                    "--consumer-group=snuba-eap_items-subscription-executor",
                    "--auto-offset-reset=latest",
                    f"--log-level={log_level}",
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
                    "--followed-consumer-group=errors_group",
                    "--auto-offset-reset=latest",
                    "--no-strict-offset-reset",
                    f"--log-level={log_level}",
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
                    f"--log-level={log_level}",
                    "--schedule-ttl=10",
                    "--stale-threshold-seconds=900",
                ],
            ),
            (
                "subscriptions-scheduler-executor-eap-spans",
                [
                    "snuba",
                    "subscriptions-scheduler-executor",
                    "--dataset=events_analytics_platform",
                    "--entity=eap_items_span",
                    "--consumer-group=snuba-eap_spans-subscription-executor",
                    "--followed-consumer-group=eap_items_span_group",
                    "--auto-offset-reset=latest",
                    "--no-strict-offset-reset",
                    f"--log-level={log_level}",
                    "--schedule-ttl=10",
                    "--stale-threshold-seconds=900",
                ],
            ),
            (
                "subscriptions-scheduler-executor-eap-items",
                [
                    "snuba",
                    "subscriptions-scheduler-executor",
                    "--dataset=events_analytics_platform",
                    "--entity=eap_items",
                    "--consumer-group=snuba-eap_items-subscriptions-scheduler",
                    "--followed-consumer-group=eap_items_group",
                    "--auto-offset-reset=latest",
                    "--no-strict-offset-reset",
                    f"--log-level={log_level}",
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
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
                ],
            ),
            (
                "generic-metrics-distributions-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=generic_metrics_distributions_raw",
                    "--consumer-group=snuba-gen-metrics-distributions-consumers",
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
                ],
            ),
            (
                "generic-metrics-sets-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=generic_metrics_sets_raw",
                    "--consumer-group=snuba-gen-metrics-sets-consumers",
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
                ],
            ),
            (
                "generic-metrics-counters-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=generic_metrics_counters_raw",
                    "--consumer-group=snuba-gen-metrics-counters-consumers",
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
                ],
            ),
            (
                "generic-metrics-gauges-consumer",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=generic_metrics_gauges_raw",
                    "--consumer-group=snuba-gen-metrics-gauges-consumers",
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
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
                            f"--log-level={log_level}",
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
                            f"--log-level={log_level}",
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
                            "--followed-consumer-group=snuba-gen-metrics-distributions-consumers",
                            "--auto-offset-reset=latest",
                            f"--log-level={log_level}",
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
                            "--followed-consumer-group=snuba-gen-metrics-sets-consumers",
                            "--auto-offset-reset=latest",
                            f"--log-level={log_level}",
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
                            "--followed-consumer-group=snuba-gen-metrics-counters-consumers",
                            "--auto-offset-reset=latest",
                            f"--log-level={log_level}",
                            "--schedule-ttl=10",
                        ],
                    ),
                    (
                        "subscriptions-scheduler-generic-metrics-gauges",
                        [
                            "snuba",
                            "subscriptions-scheduler",
                            "--entity=generic_metrics_gauges",
                            "--consumer-group=snuba-generic-metrics-gauges-subscriptions-schedulers",
                            "--followed-consumer-group=snuba-gen-metrics-gauges-consumers",
                            "--auto-offset-reset=latest",
                            f"--log-level={log_level}",
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
                            f"--log-level={log_level}",
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
                            f"--log-level={log_level}",
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
                    "--consumer-group=profiles_group",
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
                ],
            ),
            (
                "profile_chunks",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=profile_chunks",
                    "--consumer-group=profile_chunks_group",
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
                ],
            ),
            (
                "functions",
                [
                    "snuba",
                    "rust-consumer",
                    "--storage=functions_raw",
                    "--consumer-group=functions_group",
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
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
                    "--consumer-group=replays_group",
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
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
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
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
                    *COMMON_RUST_CONSUMER_DEV_OPTIONS,
                    f"--log-level={log_level}",
                ],
            ),
        ]

    if settings.ENABLE_LW_DELETIONS_CONSUMER:
        daemons += [
            (
                "lw-deletions-consumer",
                [
                    "snuba",
                    "lw-deletions-consumer",
                    "--storage=search_issues",
                    "--consumer-group=search_issues_deletes_group",
                    "--max-rows-batch-size=10",
                    "--max-batch-time-ms=1000",
                    "--auto-offset-reset=latest",
                    "--no-strict-offset-reset",
                    f"--log-level={log_level}",
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
