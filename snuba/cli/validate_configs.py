from glob import glob

import click
from jsonschema.exceptions import ValidationError

from snuba import settings
from snuba.datasets.configuration.json_schema import V1_ALL_SCHEMAS
from snuba.datasets.configuration.loader import load_configuration_data


@click.command()
def validate_configs() -> None:
    print("Validating configs:")
    errors = []

    for config_file in glob(f"{settings.CONFIG_FILES_PATH}/**/*.yaml", recursive=True):
        file_name = config_file[len(settings.CONFIG_FILES_PATH) :]
        message = f"Validating: {file_name}..."
        try:
            load_configuration_data(config_file, V1_ALL_SCHEMAS)
        except Exception as e:
            errors.append((file_name, e))
            message += " FAILED"
        print(message)

    if errors:
        print("\nFailures:")
        for file_name, err in errors:
            print(f"{file_name}: ", end="")
            if isinstance(err, ValidationError):
                print(err.message)
            else:
                print(f"{err.__class__.__name__}: {err}")
        exit(1)
    else:
        print("All configs valid!")
