import subprocess
import sys
from tempfile import TemporaryDirectory

import jsonschema2md

from snuba import settings
from snuba.datasets.configuration.json_schema import V1_ALL_SCHEMAS

CONFIGURATION_DOCS_PATH = f"{settings.ROOT_REPO_DIRECTORY}/docs/source/configuration"


def execute_bash_command(command: str) -> None:
    try:
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    for component, schema in V1_ALL_SCHEMAS.items():
        with TemporaryDirectory() as temp_dir:
            temp_filename = f"{temp_dir}/{component}"
            parser = jsonschema2md.Parser(
                examples_as_yaml=False,
                show_examples="all",
            )
            md_lines = parser.parse_schema(schema)
            md_string = "".join(md_lines).replace(
                "`", ""
            )  # restructuredtext does not support nested inline markup, remove literals
            with open(f"{temp_filename}.md", "w") as md_file:
                md_file.write(md_string)
            execute_bash_command(
                f"pandoc --from=markdown --to=rst --output={CONFIGURATION_DOCS_PATH}/{component}.rst {temp_filename}.md"
            )

            # import pypandoc
            # from pypandoc.pandoc_download import download_pandoc

            # download_pandoc()  # https://github.com/JessicaTegner/pypandoc/issues/295
            # output = pypandoc.convert_file(f"{temp_filename}.md", "rst")
            # with open(f"{CONFIGURATION_DOCS_PATH}/{component}.rst", "w") as rst_file:
            #     rst_file.write(output)

    print("Successfully generated config schema docs in `docs/source/configuration`")
