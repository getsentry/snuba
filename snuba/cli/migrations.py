import click
import os

from typing import Sequence

from snuba.clusters.cluster import ClickhouseNodeType, CLUSTERS
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.connect import check_clickhouse_connections
from snuba.migrations.errors import MigrationError
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status


@click.group()
def migrations() -> None:
    pass


@migrations.command()
def list() -> None:
    """
    Lists migrations and their statuses
    """
    check_clickhouse_connections()
    runner = Runner()
    for group, group_migrations in runner.show_all():
        click.echo(group.value)
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
@click.option("--force", is_flag=True)
def migrate(force: bool) -> None:
    """
    Runs all migrations. Blocking migrations will not be run unless --force is passed.
    """
    check_clickhouse_connections()
    runner = Runner()

    try:
        runner.run_all(force=force)
    except MigrationError as e:
        raise click.ClickException(str(e))

    click.echo("Finished running migrations")


@migrations.command()
@click.option("--group", required=True, help="Migration group")
@click.option("--migration-id", required=True, help="Migration ID")
@click.option("--force", is_flag=True)
@click.option("--fake", is_flag=True)
@click.option("--dry-run", is_flag=True)
def run(group: str, migration_id: str, force: bool, fake: bool, dry_run: bool) -> None:
    """
    Runs a single migration.
    --force must be passed in order to run blocking migrations.
    --fake marks a migration as completed without running anything.

    Migrations that are already in an in-progress or completed status will not be run.
    """
    if not dry_run:
        check_clickhouse_connections()

    runner = Runner()
    migration_group = MigrationGroup(group)
    migration_key = MigrationKey(migration_group, migration_id)

    if dry_run:
        runner.run_migration(migration_key, dry_run=True)
        return

    try:
        if fake:
            click.confirm(
                "This will mark the migration as completed without actually running it. Your database may be in an invalid state. Are you sure?",
                abort=True,
            )
        runner.run_migration(migration_key, force=force, fake=fake)
    except MigrationError as e:
        raise click.ClickException(str(e))

    click.echo(f"Finished running migration {migration_key}")


@migrations.command()
@click.option("--group", required=True, help="Migration group")
@click.option("--migration-id", required=True, help="Migration ID")
@click.option("--force", is_flag=True)
@click.option("--fake", is_flag=True)
@click.option("--dry-run", is_flag=True)
def reverse(
    group: str, migration_id: str, force: bool, fake: bool, dry_run: bool
) -> None:
    """
    Reverses a single migration.

    --force is required to reverse an already completed migration.
    --fake marks a migration as reversed without doing anything.
    """
    if not dry_run:
        check_clickhouse_connections()
    runner = Runner()
    migration_group = MigrationGroup(group)
    migration_key = MigrationKey(migration_group, migration_id)

    if dry_run:
        runner.reverse_migration(migration_key, dry_run=True)
        return

    try:
        if fake:
            click.confirm(
                "This will mark the migration as not started without actually reversing it. Your database may be in an invalid state. Are you sure?",
                abort=True,
            )
        runner.reverse_migration(migration_key, force=force, fake=fake)
    except MigrationError as e:
        raise click.ClickException(str(e))

    click.echo(f"Finished reversing migration {migration_key}")


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
    default=os.environ.get("CLICKHOUSE_HOST", "localhost"),
)
@click.option(
    "--port",
    type=int,
    required=True,
    default=int(os.environ.get("CLICKHOUSE_PORT", 9000)),
)
@click.option(
    "--user",
    type=str,
    required=True,
    default=os.environ.get("CLICKHOUSE_USER", "default"),
)
@click.option(
    "--password",
    type=str,
    required=True,
    default=os.environ.get("CLICKHOUSE_PASSWORD", ""),
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
    user: str,
    password: str,
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
