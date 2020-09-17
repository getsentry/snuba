import click

from snuba.migrations.connect import check_clickhouse_connections
from snuba.migrations.errors import MigrationError
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status


@click.group()
def migrations() -> None:
    check_clickhouse_connections()


@migrations.command()
def list() -> None:
    """
    Lists migrations and their statuses
    """
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
def run(group: str, migration_id: str, force: bool, fake: bool) -> None:
    """
    Runs a single migration.
    --force must be passed in order to run blocking migrations.
    --fake marks a migration as completed without running anything.

    Migrations that are already in an in-progress or completed status will not be run.
    """
    runner = Runner()
    migration_group = MigrationGroup(group)
    migration_key = MigrationKey(migration_group, migration_id)

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
def reverse(group: str, migration_id: str, force: bool, fake: bool) -> None:
    """
    Reverses a single migration.

    --force is required to reverse an already completed migration.
    --fake marks a migration as reversed without doing anything.
    """
    runner = Runner()
    migration_group = MigrationGroup(group)
    migration_key = MigrationKey(migration_group, migration_id)

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
