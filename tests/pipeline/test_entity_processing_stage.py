import importlib
from collections.abc import Generator

import pytest

from snuba import settings
from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
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
from snuba.pipeline.query_pipeline import QueryPipelineData, QueryPipelineResult
from snuba.pipeline.stages.query_processing import EntityProcessingStage
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Storage as QueryStorage
from snuba.query.data_source.simple import Table
from snuba.query.dsl import and_cond, column, equals, literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.parser import parse_snql_query_initial
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.utils.schemas import Column as ColumnSchema
from snuba.utils.schemas import DateTime, UInt

"""
Create a fake storage and entity, put the entity in global definitions
"""

_MOCK_STORAGE_KEY = StorageKey("mockstorage")


def _create_pipe_input_from_snql(snql_query: str) -> QueryPipelineData[Request]:
    query_body = {"query": snql_query}
    logical_query = parse_snql_query_initial(query_body["query"])
    query_settings = HTTPQuerySettings()
    timer = Timer("test")
    request = Request(
        id="",
        original_body=query_body,
        query=logical_query,
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
    )
    return QueryPipelineData(
        data=request,
        query_settings=request.query_settings,
        timer=timer,
        error=None,
    )


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


def test_basic(
    mock_storage: ReadableTableStorage, mock_entity: PluggableEntity
) -> None:
    pipe_input = _create_pipe_input_from_snql(
        f"MATCH ({mock_entity.entity_key.value}) "
        "SELECT timestamp "
        "WHERE "
        "org_id = 1 AND "
        "project_id = 1"
    )

    logical_query = pipe_input.data.query
    actual = EntityProcessingStage().execute(pipe_input)
    schema = mock_storage.get_schema()
    assert isinstance(schema, TableSchema)

    assert isinstance(logical_query, LogicalQuery)
    expected = QueryPipelineResult(
        data=Query(
            from_clause=Table(
                table_name=schema.get_table_name(),
                schema=mock_storage.get_schema().get_columns(),
                storage_key=mock_storage.get_storage_key(),
                allocation_policies=mock_storage.get_allocation_policies(),
                final=logical_query.get_final(),
                sampling_rate=logical_query.get_sample(),
                mandatory_conditions=mock_storage.get_schema()
                .get_data_source()
                .get_mandatory_conditions(),
            ),
            selected_columns=[SelectedExpression("timestamp", column("timestamp"))],
            condition=and_cond(
                equals(literal(1), literal(1)),
                equals(column("org_id"), literal(1)),
                equals(column("project_id"), literal(1)),
            ),
            limit=1000,
        ),
        query_settings=pipe_input.query_settings,
        timer=pipe_input.timer,
        error=None,
    )
    assert actual == expected


def test_basic_storage(
    mock_storage: ReadableTableStorage, mock_query_storage: QueryStorage
) -> None:
    pipe_input = _create_pipe_input_from_snql(
        f"MATCH STORAGE({mock_query_storage.key.value}) "
        "SELECT timestamp "
        "WHERE "
        "org_id = 1 AND "
        "project_id = 1"
    )

    schema = mock_storage.get_schema()
    actual = EntityProcessingStage().execute(pipe_input)
    logical_query = pipe_input.data.query
    assert isinstance(logical_query, LogicalQuery)
    assert isinstance(schema, TableSchema)

    expected = QueryPipelineResult(
        data=Query(
            from_clause=Table(
                table_name=schema.get_table_name(),
                schema=mock_storage.get_schema().get_columns(),
                storage_key=mock_storage.get_storage_key(),
                allocation_policies=mock_storage.get_allocation_policies(),
                final=logical_query.get_final(),
                sampling_rate=logical_query.get_sample(),
                mandatory_conditions=mock_storage.get_schema()
                .get_data_source()
                .get_mandatory_conditions(),
            ),
            selected_columns=[SelectedExpression("timestamp", column("timestamp"))],
            condition=and_cond(
                # NOTE: no query processor was applied here, this is because the storage query does not run
                # entity processors (there is no entity)
                equals(column("org_id"), literal(1)),
                equals(column("project_id"), literal(1)),
            ),
            limit=1000,
        ),
        query_settings=pipe_input.query_settings,
        timer=pipe_input.timer,
        error=None,
    )
    assert repr(actual.data) == repr(expected.data)
    assert actual == expected
