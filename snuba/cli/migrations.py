import re
from typing import Optional, Sequence

import click

import snuba.migrations.autogeneration as autogeneration
from snuba.clusters.cluster import CLUSTERS
from snuba.datasets.readiness_state import ReadinessState
from snuba.environment import setup_logging
from snuba.migrations.connect import (
    check_clickhouse_connections,
    get_clickhouse_clusters_for_migration_group,
    get_clusters_for_readiness_states,
)
from snuba.migrations.errors import MigrationError
from snuba.migrations.groups import MigrationGroup, get_group_readiness_state
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status

LOG_LEVELS = ["critical", "error", "warning", "info", "debug", "notset"]


@click.group()
def migrations() -> None:
    pass


@migrations.command()
def list() -> None:
    """
    Lists migrations and their statuses
    """
    setup_logging()
    check_clickhouse_connections(CLUSTERS)
    runner = Runner()
    for group, group_migrations in runner.show_all(include_nonexistent=True):
        readiness_state = get_group_readiness_state(group)
        click.echo(f"{group.value} (readiness_state: {readiness_state.value})")
        for migration_id, status, blocking, existing in group_migrations:
            symbol = {
                Status.COMPLETED: "X",
                Status.NOT_STARTED: " ",
                Status.IN_PROGRESS: "-",
            }[status]

            in_progress_text = " (IN PROGRESS)" if status == Status.IN_PROGRESS else ""

            blocking_text = ""
            if status != Status.COMPLETED and blocking:
                blocking_text = " (blocking)"

            existing_text = "" if existing else " (this migration no longer exists)"

            click.echo(
                f"[{symbol}]  {migration_id}{in_progress_text}{blocking_text}{existing_text}"
            )

        click.echo()


@migrations.command()
@click.option("-g", "--group", default=None)
@click.option(
    "-r",
    "--readiness-state",
    multiple=True,
    type=click.Choice([r.value for r in ReadinessState], case_sensitive=False),
    default=None,
)
@click.argument("through", default="all")
@click.option("--force", is_flag=True)
@click.option("--fake", is_flag=True)
@click.option("--check-dangerous", is_flag=True)
@click.option(
    "--log-level", help="Logging level to use.", type=click.Choice(LOG_LEVELS)
)
def migrate(
    group: Optional[str],
    readiness_state: Optional[Sequence[str]],
    through: str,
    force: bool,
    fake: bool,
    check_dangerous: bool,
    log_level: Optional[str] = None,
) -> None:
    """
    If group is specified, runs all the migrations for a group (including any pending
    system migrations), otherwise runs all migrations for all groups.

    Blocking migrations will not be run unless --force is passed.
    """

    readiness_states = (
        [ReadinessState(state) for state in readiness_state]
        if readiness_state
        else None
    )

    setup_logging(log_level)
    clusters_to_check = (
        get_clusters_for_readiness_states(readiness_states, CLUSTERS)
        if readiness_states
        else CLUSTERS
    )
    check_clickhouse_connections(clusters_to_check)
    runner = Runner()

    try:
        if group:
            migration_group = MigrationGroup(group)
        else:
            if through != "all":
                raise click.ClickException("Need migration group")
            migration_group = None
        runner.run_all(
            through=through,
            force=force,
            fake=fake,
            group=migration_group,
            readiness_states=readiness_states,
            check_dangerous=check_dangerous,
        )
    except MigrationError as e:
        raise click.ClickException(str(e))

    click.echo("Finished running migrations")


@migrations.command()
@click.option("-g", "--group", default=None)
@click.option(
    "-r",
    "--readiness-state",
    multiple=True,
    type=click.Choice([r.value for r in ReadinessState], case_sensitive=False),
    default=None,
)
@click.argument("through", default="all")
@click.option("--force", is_flag=True)
@click.option("--fake", is_flag=True)
@click.option("--include-system", is_flag=True)
@click.option(
    "--log-level", help="Logging level to use.", type=click.Choice(LOG_LEVELS)
)
def revert(
    group: Optional[str],
    readiness_state: Optional[Sequence[str]],
    through: str,
    force: bool,
    fake: bool,
    include_system: bool,
    log_level: Optional[str] = None,
) -> None:
    """
    If group is specified, reverse all the migrations for a group.
    If no group is specified, reverses all migrations for all groups.
        * by default SYSTEM migrations are NOT reversed
        * use --include-system to also reverse SYSTEM migrations

    --force is required
    """

    readiness_states = (
        [ReadinessState(state) for state in readiness_state]
        if readiness_state
        else None
    )

    setup_logging(log_level)
    clusters_to_check = (
        get_clusters_for_readiness_states(readiness_states, CLUSTERS)
        if readiness_states
        else CLUSTERS
    )
    check_clickhouse_connections(clusters_to_check)
    runner = Runner()

    try:
        if group:
            migration_group = MigrationGroup(group)
        else:
            if through != "all":
                raise click.ClickException("Need migration group")
            migration_group = None
        runner.reverse_all(
            through=through,
            force=force,
            fake=fake,
            include_system=include_system,
            group=migration_group,
            readiness_states=readiness_states,
        )
    except MigrationError as e:
        raise click.ClickException(str(e))

    click.echo("Finished reversing migrations")


@migrations.command()
@click.option("--group", required=True, help="Migration group")
@click.option("--migration-id", required=True, help="Migration ID")
@click.option("--force", is_flag=True)
@click.option("--fake", is_flag=True)
@click.option("--dry-run", is_flag=True)
@click.option("--yes", is_flag=True)
@click.option("--check-dangerous", is_flag=True)
@click.option(
    "--log-level", help="Logging level to use.", type=click.Choice(LOG_LEVELS)
)
def run(
    group: str,
    migration_id: str,
    force: bool,
    fake: bool,
    dry_run: bool,
    yes: bool,
    check_dangerous: bool,
    log_level: Optional[str] = None,
) -> None:
    """
    Runs a single migration.
    --force must be passed in order to run blocking migrations.
    --fake marks a migration as completed without running anything.

    Migrations that are already in an in-progress or completed status will not be run.
    """
    setup_logging(log_level)
    migration_group = MigrationGroup(group)
    if not dry_run:
        # just check the connection for the migration that's being run
        check_clickhouse_connections(
            get_clickhouse_clusters_for_migration_group(migration_group)
        )

    runner = Runner()
    migration_key = MigrationKey(migration_group, migration_id)

    if dry_run:
        runner.run_migration(
            migration_key, dry_run=True, check_dangerous=check_dangerous
        )
        return

    try:
        if fake and not yes:
            click.confirm(
                "This will mark the migration as completed without actually running it. Your database may be in an invalid state. Are you sure?",
                abort=True,
            )
        runner.run_migration(
            migration_key, force=force, fake=fake, check_dangerous=check_dangerous
        )
    except MigrationError as e:
        raise click.ClickException(str(e))

    click.echo(f"Finished running migration {migration_key}")


@migrations.command()
@click.option("--group", required=True, help="Migration group")
@click.option("--migration-id", required=True, help="Migration ID")
@click.option("--force", is_flag=True)
@click.option("--fake", is_flag=True)
@click.option("--dry-run", is_flag=True)
@click.option("--yes", is_flag=True)
@click.option(
    "--log-level", help="Logging level to use.", type=click.Choice(LOG_LEVELS)
)
def reverse(
    group: str,
    migration_id: str,
    force: bool,
    fake: bool,
    dry_run: bool,
    yes: bool,
    log_level: Optional[str] = None,
) -> None:
    """
    Reverses a single migration.

    --force is required to reverse an already completed migration.
    --fake marks a migration as reversed without doing anything.
    """
    migration_group = MigrationGroup(group)
    setup_logging(log_level)
    if not dry_run:
        check_clickhouse_connections(
            get_clickhouse_clusters_for_migration_group(migration_group)
        )
    runner = Runner()
    migration_key = MigrationKey(migration_group, migration_id)

    if dry_run:
        runner.reverse_migration(migration_key, dry_run=True)
        return

    try:
        if fake and not yes:
            click.confirm(
                "This will mark the migration as not started without actually reversing it. Your database may be in an invalid state. Are you sure?",
                abort=True,
            )
        runner.reverse_migration(migration_key, force=force, fake=fake)
    except MigrationError as e:
        raise click.ClickException(str(e))

    click.echo(f"Finished reversing migration {migration_key}")


@migrations.command()
@click.option("--group", help="Migration group")
@click.option("--fake", is_flag=True)
@click.option("--dry-run", is_flag=True)
@click.option("--yes", is_flag=True)
@click.option(
    "--log-level", help="Logging level to use.", type=click.Choice(LOG_LEVELS)
)
def reverse_in_progress(
    fake: bool,
    dry_run: bool,
    yes: bool,
    group: Optional[str] = None,
    log_level: Optional[str] = None,
) -> None:
    """
    Reverses any in progress migrations for all migration groups.
    If group is specified, only reverse in progress migrations for
    that group.

    --fake marks migrations as reversed without doing anything.
    """
    setup_logging(log_level)
    migration_group = MigrationGroup(group) if group else None
    if not dry_run:
        clusters_to_check = (
            CLUSTERS
            if not migration_group
            else get_clickhouse_clusters_for_migration_group(migration_group)
        )
        check_clickhouse_connections(clusters_to_check)
    runner = Runner()

    if dry_run:
        runner.reverse_in_progress(group=migration_group, dry_run=True)
        return

    try:
        if fake and not yes:
            click.confirm(
                "This will mark the migration as not started without actually reversing it. Your database may be in an invalid state. Are you sure?",
                abort=True,
            )
        runner.reverse_in_progress(group=migration_group, fake=fake)
    except MigrationError as e:
        raise click.ClickException(str(e))

    click.echo("Finished reversing in progress migrations")


@migrations.command()
@click.argument("storage_path", type=str)
@click.option("--name", type=str, help="optional name for the migration")
def generate(storage_path: str, name: Optional[str] = None) -> None:
    """
    Given a path to user-modified storage.yaml definition (inside snuba/datasets/configuration/*/storages/*.yaml),
    and an optional name for the migration,
    generates a snuba migration based on the schema modifications to the storage.yaml.

    Currently only column addition is supported.

    The migration is generated based on the diff between HEAD and working dir. Therefore modifications to the
    storage should be uncommitted in the working dir.

    The generated migration will be written into the local directory. The user is responsible for making
    the commit, PR, and merging.

    see MIGRATIONS.md in the root folder for more info
    """
    expected_pattern = r"(.+/)?snuba/datasets/configuration/.*/storages/.*\.(yml|yaml)"
    if not re.fullmatch(expected_pattern, storage_path):
        raise click.ClickException(
            f"Storage path {storage_path} does not match expected pattern {expected_pattern}"
        )

    path = autogeneration.generate(storage_path, migration_name=name)
    click.echo(f"Migration successfully generated at {path}")
