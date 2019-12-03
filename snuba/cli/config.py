import click
import json

from snuba import state


def human_fmt(values):
    lines = []
    for k, v in values.items():
        lines.append(f"{k} = {v!r} ({type(v).__name__})")
    return "\n".join(lines)


def json_fmt(values):
    return json.dumps(values)


def get_user():
    import getpass

    return f"{getpass.getuser()} (cli)"


FORMATS = {"human": human_fmt, "json": json_fmt}


@click.group()
def config():
    "Manage runtime configuration."


@config.command("get-all")
@click.option("--format", type=click.Choice(FORMATS.keys()), default="human")
def get_all(*, format):
    "Dump all runtime configuration."

    rv = state.get_raw_configs()
    click.echo(FORMATS[format](rv))


@config.command()
@click.option("--format", type=click.Choice(FORMATS.keys()), default="human")
@click.argument("key")
def get(*, key, format):
    "Get a single key."

    try:
        rv = state.get_raw_configs()[key]
    except KeyError:
        raise click.ClickException(f"Key {key!r} not found.")
    click.echo(FORMATS[format]({key: rv}))


@config.command()
@click.argument("key")
@click.argument("value")
def set(*, key, value):
    "Set a single key."

    state.set_config(key, value, user=get_user())


@config.command("set-many")
@click.argument("data")
def set_many(*, data):
    "Set multiple keys, input as JSON."

    state.set_configs(json.loads(data), user=get_user())


@config.command()
@click.argument("key")
def delete(*, key):
    "Delete a single key."

    try:
        rv = state.get_raw_configs()[key]
    except KeyError:
        raise click.ClickException(f"Key {key!r} not found.")

    click.echo(human_fmt({key: rv}))
    click.confirm(f"\nAre you sure you want to delete this?", abort=True)

    state.delete_config(key, user=get_user())


@config.command()
def log():
    "Dump the config change log."
    from datetime import datetime

    for key, (ts, user, before, after) in state.get_config_changes():
        click.echo(
            f"{datetime.fromtimestamp(int(ts)).isoformat()}: "
            f"key={key!r} user={user!r} "
            f"before={before!r} after={after!r}"
        )
