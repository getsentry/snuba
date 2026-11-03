import click

from snuba.subscriptions.postgres.database import migrate_to_head


@click.group()
def subscriptions_postgres() -> None:
    pass


@subscriptions_postgres.command()
def migrate() -> None:
    """Upgrade the subscriptions Postgres database to the latest migration."""
    migrate_to_head()
