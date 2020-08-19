import click

from snuba.migrations.connect import check_clickhouse_connections
from snuba.migrations.runner import Runner
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
