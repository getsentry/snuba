from typing import Optional

import click

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import InvalidEntityError, get_entity
from snuba.utils.describer import Description, DescriptionVisitor, Property


class CLIDescriber(DescriptionVisitor):
    def __init__(self) -> None:
        self.__current_indentation = 0

    def __indent(self) -> str:
        return " " * self.__current_indentation

    def visit_header(self, header: Optional[str]) -> None:
        if header is not None:
            click.echo(f"{self.__indent()}{header}")
            click.echo(f"{self.__indent()}--------------------------------")

    def visit_description(self, desc: Description) -> None:
        self.__current_indentation += 1
        desc.accept(self)
        self.__current_indentation -= 1
        click.echo("")

    def visit_string(self, string: str) -> None:
        click.echo(f"{self.__indent()}{string}")

    def visit_property(self, property: Property) -> None:
        click.echo(f"{self.__indent()}{property.name}: {property.value}")


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
@click.argument(
    "entity_name",
    type=click.Choice([entity.value for entity in EntityKey]),
)
def describe(entity_name: str) -> None:
    try:
        entity = get_entity(EntityKey(entity_name))
        click.echo(f"Entity {entity_name}")
        entity.describe().accept(CLIDescriber())
    except InvalidEntityError:
        click.echo(f"Entity {entity_name} does not exists or it is not registered.")
