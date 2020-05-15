from typing import Iterator

import pytest

from snuba.datasets.dataset import Dataset
from tests.base import dataset_manager


@pytest.fixture(scope="module", params=["events", "transactions"])
def dataset(request) -> Iterator[Dataset]:
    with dataset_manager(request.param) as instance:
        yield instance
