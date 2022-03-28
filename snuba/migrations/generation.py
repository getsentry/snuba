import datetime
from dataclasses import dataclass

from jinja2 import Environment, PackageLoader, select_autoescape

from snuba.migrations.groups import MigrationGroup

env = Environment(
    loader=PackageLoader("snuba", package_path="migrations"),
    autoescape=select_autoescape(),
)

template = env.get_template("migration_template.py.jinja")


@dataclass(frozen=True)
class GenerationResult:
    filename: str
    groups_entry: str


def generate(group_enum: MigrationGroup, description: str) -> GenerationResult:
    group = group_enum.value
    transformed_description = description.lower().replace(" ", "_")
    filesystem_friendly_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    entry = f"{filesystem_friendly_timestamp}_{transformed_description}"
    output_file = f"snuba/migrations/snuba_migrations/{group}/{entry}.py"
    template.stream({"group": group, "description": description}).dump(output_file)
    return GenerationResult(filename=output_file, groups_entry=entry)
