from glob import glob

from snuba import settings
from snuba.datasets.configuration.json_schema import V1_ALL_SCHEMAS
from snuba.datasets.configuration.loader import load_configuration_data


def test_validate_all_configs() -> None:
    for config_file in glob(f"{settings.CONFIG_FILES_PATH}/**/*.yaml", recursive=True):
        load_configuration_data(config_file, V1_ALL_SCHEMAS)
