import os
from typing import Optional, Sequence

import click

from snuba.clusters.cluster import CLUSTERS, ClickhouseNodeType
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.environment import setup_logging
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
    runner = Runner()
    for group, group_migrations in runner.show_all():
        readiness_state = get_group_readiness_state(group)
        click.echo(f"{group.value} (readiness_state: {readiness_state.value})")
        for migration_id, status, blocking in group_migrations:
            symbol = {
                Status.COMPLETED: "X",
                Status.NOT_STARTED: " ",
                Status.IN_PROGRESS: "-",
            }[status]

            in_progress_text = " (IN PROGRESS)" if status == Status.IN_PROGRESS else ""

            blocking_text = ""
            if status != Status.COMPLETED and blocking:
                blocking_text = " (blocking)"

            click.echo(f"[{symbol}]  {migration_id}{in_progress_text}{blocking_text}")

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
    setup_logging(log_level)
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
            readiness_states=(
                [ReadinessState(state) for state in readiness_state]
                if readiness_state
                else None
            ),
            check_dangerous=check_dangerous,
        )
    except MigrationError as e:
        raise click.ClickException(str(e))

    click.echo("Finished running migrations")


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

    runner = Runner()
    migration_group = MigrationGroup(group)
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
    setup_logging(log_level)
    runner = Runner()
    migration_group = MigrationGroup(group)
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
    runner = Runner()

    migration_group = MigrationGroup(group) if group else None

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
@click.option(
    "--type", "node_type", type=click.Choice(["local", "dist"]), required=True
)
@click.option(
    "--storage-set",
    "storage_set_names",
    type=click.Choice([s.value for s in StorageSetKey]),
    required=True,
    multiple=True,
)
@click.option(
    "--host-name",
    type=str,
    required=True,
    default=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
)
@click.option(
    "--port",
    type=int,
    required=True,
    default=int(os.environ.get("CLICKHOUSE_PORT", 9000)),
)
@click.option(
    "--database",
    type=str,
    required=True,
    default=os.environ.get("CLICKHOUSE_DATABASE", "default"),
)
def add_node(
    node_type: str,
    storage_set_names: Sequence[str],
    host_name: str,
    port: int,
    database: str,
) -> None:
    """
    Runs all migrations on a brand new ClickHouse node. This should be performed
    before a new node is added to an existing ClickHouse cluster.

    All of the SQL operations for the provided storage sets will be run. Any non
    SQL (Python) operations will be skipped.

    This operation does not change the migration status in the migrations_local
    / migrations_dist tables, since it is designed to bring a new node up to
    the same state as existing ones already added to the cluster.
    """
    user = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")

    storage_set_keys = [StorageSetKey(name) for name in storage_set_names]

    cluster = next(
        (
            c
            for c in CLUSTERS
            if all(ss in c.get_storage_set_keys() for ss in storage_set_keys)
        ),
        None,
    )

    if not cluster:
        raise click.ClickException("Storage sets should be in the same cluster")

    if cluster.is_single_node():
        raise click.ClickException("You cannot add a node to a single node cluster")

    Runner.add_node(
        node_type=ClickhouseNodeType(node_type),
        storage_sets=storage_set_keys,
        host_name=host_name,
        port=port,
        user=user,
        password=password,
        database=database,
    )
