import click

from snuba.migrations.runner import Runner
from snuba.migrations.status import Status


@click.group()
def migrations() -> None:
    """
    Currently only for development use
    """
    click.echo("Warning: The migrations tool is currently only for development use\n")


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
                Status.COMPLETED: "x",
                Status.NOT_STARTED: " ",
                Status.IN_PROGRESS: "?",
            }[status]

            blocking_text = ""
            if status != Status.COMPLETED and blocking:
                blocking_text = " (blocking)"

            click.echo(f"[{symbol}]  " f"{migration_id}" f"{blocking_text}")

        click.echo()
