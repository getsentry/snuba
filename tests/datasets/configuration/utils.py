from typing import Generator

import pytest

from snuba.datasets.factory import reset_dataset_factory


class ConfigurationTest:
    @pytest.fixture(autouse=True)
    def reset_configuration(self) -> Generator[None, None, None]:
        reset_dataset_factory()
        yield
