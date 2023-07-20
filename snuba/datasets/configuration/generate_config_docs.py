from tempfile import TemporaryDirectory

import jsonschema2md

from snuba import settings
from snuba.datasets.configuration.json_schema import V1_ALL_SCHEMAS

CONFIGURATION_DOCS_PATH = f"{settings.ROOT_REPO_PATH}/docs/source/configuration"


if __name__ == "__main__":
    for component, schema in V1_ALL_SCHEMAS.items():
        with TemporaryDirectory() as temp_dir:
            parser = jsonschema2md.Parser(
                examples_as_yaml=False,
                show_examples="all",
            )
            md_lines = parser.parse_schema(schema)
            reformatted_md_lines = []
            for line in md_lines:
                # jsonschema2md adds some extra lines of its own. This removes them.
                if "**Items**" not in line:
                    line = line.replace("`", "")
                    line = line.replace(" Cannot contain additional properties.", "")
                    reformatted_md_lines.append(line)

            with open(f"{CONFIGURATION_DOCS_PATH}/{component}.md", "w") as md_file:
                md_file.write("".join(reformatted_md_lines))

    print("Successfully generated config schema docs in `docs/source/configuration`")
