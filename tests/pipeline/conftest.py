import importlib
from collections.abc import Generator

import pytest

from snuba import settings
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.clusters import cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import override_entity_map, reset_entity_factory
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
)
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import EntityStorageConnection, ReadableTableStorage
from snuba.datasets.storages import factory
from snuba.datasets.storages.factory import get_config_built_storages
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.simple import Storage as QueryStorage
from snuba.query.dsl import equals, literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.utils.schemas import Column as ColumnSchema
from snuba.utils.schemas import DateTime, UInt

_MOCK_STORAGE_KEY = StorageKey("mockstorage")


class NoopQueryProcessor(LogicalQueryProcessor):
    def process_query(self, query: LogicalQuery, query_settings: QuerySettings) -> None:
        query.add_condition_to_ast(equals(literal(1), literal(1)))


@pytest.fixture
def mock_query_storage() -> Generator[QueryStorage, None, None]:
    yield QueryStorage(key=_MOCK_STORAGE_KEY)


@pytest.fixture
def mock_storage() -> Generator[ReadableTableStorage, None, None]:
    # create a storage
    storkey = _MOCK_STORAGE_KEY
    storsetkey = StorageSetKey("mockstorageset")
    storage = ReadableTableStorage(
        storage_key=storkey,
        storage_set_key=storsetkey,
        schema=TableSchema(
            columns=ColumnSet(
                [
                    ColumnSchema("org_id", UInt(64)),
                    ColumnSchema("project_id", UInt(64)),
                    ColumnSchema("timestamp", DateTime()),
                ]
            ),
            local_table_name=f"{storkey.value}_local",
            dist_table_name=f"{storkey.value}_dist",
            storage_set_key=storsetkey,
        ),
        readiness_state=ReadinessState.COMPLETE,
    )
    # add it to the global storages and clusters
    get_config_built_storages()[storage.get_storage_key()] = storage
    assert len(settings.CLUSTERS) == 1
    settings.CLUSTERS[0]["storage_sets"].add("mockstorageset")
    importlib.reload(cluster)
    yield storage
    # teardown
    importlib.reload(settings)
    importlib.reload(cluster)
    importlib.reload(factory)


@pytest.fixture
def mock_entity(
    mock_storage: ReadableTableStorage,
) -> Generator[PluggableEntity, None, None]:
    # setup
    entkey = EntityKey("mock_entity")
    entity = PluggableEntity(
        entity_key=entkey,
        storages=[EntityStorageConnection(mock_storage, TranslationMappers())],
        query_processors=[NoopQueryProcessor()],
        columns=mock_storage.get_schema().get_columns().columns,
        validators=[],
        required_time_column="timestamp",
        storage_selector=DefaultQueryStorageSelector(),
    )
    override_entity_map(entkey, entity)
    yield entity
    # teardown
    reset_entity_factory()
