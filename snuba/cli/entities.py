import click

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import InvalidEntityError, get_entity


@click.group()
def entities() -> None:
    pass


@entities.command()
def list() -> None:
    """
    Declared entities
    """

    click.echo("Declared Entities:")
    for r in EntityKey:
        click.echo(r.value)


@entities.command()
@click.argument("entity_name")
def describe(entity_name: str) -> None:
    try:
        entity = get_entity(EntityKey(entity_name))

        schema = entity.get_data_model()
        click.echo(f"Entity {entity_name} schema:")
        click.echo("===============================================")
        for column in schema.columns:
            click.echo(f"{column.for_schema()}")
        click.echo("===============================================")
        click.echo("")

        joins = entity.get_all_join_relationships()
        if joins:
            click.echo("Relationships:")
            click.echo("===============================================")
            for name, destination in joins.items():
                click.echo(f"Name: {name}")
                click.echo(f"Destination: {destination.rhs_entity.value}")
                click.echo(f"Type: {destination.join_type.value}")
                click.echo("Keys:")
                for lhs, rhs in destination.columns:
                    click.echo(
                        f"- {entity_name}.{lhs} = {destination.rhs_entity.value}.{rhs}"
                    )
                click.echo("----------------------------------------------")
    except InvalidEntityError:
        click.echo(f"Entity {entity_name} does not exists or it is not registered.")
