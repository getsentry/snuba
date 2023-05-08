import json
from typing import Any, Mapping

import click

from snuba import state
from snuba.state import MismatchedTypeException


def human_fmt(values: Mapping[Any, Any]) -> str:
    lines = []
    for k, v in values.items():
        lines.append(f"{k} = {v!r} ({type(v).__name__})")
    return "\n".join(lines)


def json_fmt(values: Any) -> str:
    return json.dumps(values)


def get_user() -> str:
    import getpass

    return f"{getpass.getuser()} (cli)"


FORMATS = {"human": human_fmt, "json": json_fmt}


@click.group()
def config() -> None:
    "Manage runtime configuration."


@config.command("get-all")
@click.option("--format", type=click.Choice([*FORMATS.keys()]), default="human")
def get_all(*, format: str) -> None:
    "Dump all runtime configuration."

    rv = state.get_raw_configs()
    click.echo(FORMATS[format](rv))


@config.command()
@click.option("--format", type=click.Choice([*FORMATS.keys()]), default="human")
@click.argument("key")
def get(*, key: str, format: str) -> None:
    "Get a single key."

    try:
        rv = state.get_raw_configs()[key]
    except KeyError:
        raise click.ClickException(f"Key {key!r} not found.")
    click.echo(FORMATS[format]({key: rv}))


@config.command()
@click.argument("key")
@click.argument("value")
@click.option(
    "--force-type", is_flag=True, default=False, help="Override type checking of values"
)
def set(*, key: str, value: str, force_type: bool) -> None:
    "Set a single key."
    try:
        state.set_config(key, value, user=get_user(), force=force_type)
    except MismatchedTypeException as exc:
        print(
            f"The new value type {exc.new_type} does not match the old value type {exc.original_type}. Use the force option to disable this check"
        )


@config.command("set-many")
@click.argument("data")
@click.option(
    "--force-type", is_flag=True, default=False, help="Override type checking of values"
)
def set_many(*, data: str, force_type: bool) -> None:
    "Set multiple keys, input as JSON."
    try:
        state.set_configs(json.loads(data), user=get_user(), force=force_type)
    except MismatchedTypeException as exc:
        print(
            f"Mismatched types for {exc.key}: Original type: {exc.original_type}, New type: {exc.new_type}. Use the force option to disable this check"
        )


@config.command()
@click.argument("key")
def delete(*, key: str) -> None:
    "Delete a single key."

    try:
        rv = state.get_raw_configs()[key]
    except KeyError:
        raise click.ClickException(f"Key {key!r} not found.")

    click.echo(human_fmt({key: rv}))
    click.confirm("\nAre you sure you want to delete this?", abort=True)

    state.delete_config_value(key, user=get_user())


@config.command()
def log() -> None:
    "Dump the config change log."
    from datetime import datetime

    for key, (ts, user, before, after) in state.get_config_changes_legacy():
        click.echo(
            f"{datetime.fromtimestamp(int(ts)).isoformat()}: "
            f"key={key!r} user={user!r} "
            f"before={before!r} after={after!r}"
        )
