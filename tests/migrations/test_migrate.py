from typing import Iterator

import pytest

# TODO: Remove this once querylog is in prod and no longer disabled
from snuba import settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import DATASET_NAMES
from tests.base import dataset_manager


settings.DISABLED_DATASETS = set()


def test_runs_all_migrations_without_errors() -> None:
    from snuba.migrations.migrate import run

    run()


@pytest.fixture(params=DATASET_NAMES)
def dataset(request) -> Iterator[Dataset]:
    with dataset_manager(request.param) as instance:
        yield instance


def test_no_schema_diffs(dataset: Dataset) -> None:
    from snuba.migrations.parse_schema import get_local_schema

    writable_storage = dataset.get_writable_storage()
    if not writable_storage:
        pytest.skip(f"{dataset!r} has no writable storage")

    clickhouse = writable_storage.get_cluster().get_connection(
        ClickhouseClientSettings.MIGRATE
    )
    table_writer = writable_storage.get_table_writer()
    dataset_schema = table_writer.get_schema()
    local_table_name = dataset_schema.get_local_table_name()
    local_schema = get_local_schema(clickhouse, local_table_name)

    assert not dataset_schema.get_column_differences(local_schema)
