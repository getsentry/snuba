import click

from snuba.migrations.connect import check_clickhouse_connections
from snuba.migrations.errors import MigrationDoesNotExist
from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status


@click.group()
def migrations() -> None:
    """
    Currently only for development use
    """
    click.echo("Warning: The migrations tool is currently only for development use\n")

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
@click.option("--group", required=True, help="Migration group")
@click.option("--migration-id", required=True, help="Migration ID")
@click.option("--force", is_flag=True)
def run(group: str, migration_id: str, force: bool) -> None:
    """
    Runs a single migration.
    The --force option must be passed in order to run blocking migrations.

    Migrations that are already in an in-progress or completed status will not be run.
    """
    runner = Runner()
    migration_group = MigrationGroup(group)
    migration_key = MigrationKey(migration_group, migration_id)

    pending_migrations = runner.get_pending_migrations(migration_group)

    if migration_id not in [m.migration_id for m in pending_migrations]:
        try:
            get_group_loader(migration_group).load_migration(migration_id)
        except MigrationDoesNotExist as e:
            raise click.ClickException(str(e))

        raise click.ClickException(
            f"{migration_group.value}: {migration_id} is already completed"
        )

    next_migration_id = pending_migrations[0].migration_id

    if next_migration_id != migration_id:
        raise click.ClickException(f"Earlier migrations need to be completed first")

    runner.run_migration(migration_key, force=force)
    click.echo(
        f"Finished running migration {migration_key.group.value}: {migration_key.migration_id}"
    )
