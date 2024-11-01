import importlib
from collections.abc import Generator

import pytest

from snuba import settings
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.clusters import cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages import factory
from snuba.datasets.storages.factory import get_config_built_storages
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_processing import StorageProcessingStage
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.dsl import Functions as f
from snuba.query.dsl import NestedColumn, column, equals, literal
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.utils.metrics.timer import Timer

tags = NestedColumn("tags")


class NoopCHQueryProcessor(ClickhouseQueryProcessor):
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        query.add_condition_to_ast(equals(literal(1), literal(1)))


@pytest.fixture
def mock_storage() -> Generator[ReadableTableStorage, None, None]:
    # Create a fake storage
    mock_storage = ReadableTableStorage(
        storage_key=StorageKey("mockstorage"),
        storage_set_key=StorageSetKey("mockstorageset"),
        schema=TableSchema(
            columns=ColumnSet([]),
            local_table_name="mockstoragelocal",
            dist_table_name="mockstoragedist",
            storage_set_key=StorageSetKey("mockstorageset"),
        ),
        readiness_state=ReadinessState.COMPLETE,
        query_processors=[NoopCHQueryProcessor()],
        mandatory_condition_checkers=[],
        allocation_policies=[],
    )
    # add it to the global storages and cluster
    get_config_built_storages()[mock_storage.get_storage_key()] = mock_storage
    assert len(settings.CLUSTERS) == 1
    settings.CLUSTERS[0]["storage_sets"].add("mockstorageset")
    importlib.reload(cluster)
    yield mock_storage
    # teardown
    importlib.reload(settings)
    importlib.reload(cluster)
    importlib.reload(factory)


def test_basic(mock_storage: ReadableTableStorage) -> None:
    query = Query(
        from_clause=Table(
            "dontmatter",
            ColumnSet([]),
            storage_key=mock_storage.get_storage_key(),
        ),
        selected_columns=[],
        condition=None,
    )
    timer = Timer("test")
    settings = HTTPQuerySettings()
    schema = mock_storage.get_schema()
    assert isinstance(schema, TableSchema)
    expected = QueryPipelineResult(
        data=Query(
            from_clause=Table(
                table_name=schema.get_table_name(),
                schema=mock_storage.get_schema().get_columns(),
                storage_key=mock_storage.get_storage_key(),
                allocation_policies=mock_storage.get_allocation_policies(),
                final=query.get_from_clause().final,
                sampling_rate=query.get_from_clause().sampling_rate,
                mandatory_conditions=mock_storage.get_schema()
                .get_data_source()
                .get_mandatory_conditions(),
            ),
            selected_columns=[],
            condition=equals(literal(1), literal(1)),
        ),
        timer=timer,
        query_settings=settings,
        error=None,
    )
    actual = StorageProcessingStage().execute(
        QueryPipelineResult(
            data=query,
            timer=timer,
            query_settings=settings,
            error=None,
        )
    )
    assert actual == expected


def test_default_subscriptable(mock_storage: ReadableTableStorage) -> None:
    query = Query(
        from_clause=Table(
            "dontmatter",
            ColumnSet([]),
            storage_key=mock_storage.get_storage_key(),
        ),
        selected_columns=[SelectedExpression('tags["something"]', tags["something"])],
        condition=None,
    )
    result = StorageProcessingStage().execute(
        QueryPipelineResult(
            data=query,
            timer=Timer("test"),
            query_settings=HTTPQuerySettings(),
            error=None,
        )
    )
    assert not result.error
    expected = Query(
        from_clause=Table(
            table_name=mock_storage.get_schema().get_table_name(),  # type: ignore
            schema=mock_storage.get_schema().get_columns(),
            storage_key=mock_storage.get_storage_key(),
            allocation_policies=mock_storage.get_allocation_policies(),
            final=query.get_from_clause().final,
            sampling_rate=query.get_from_clause().sampling_rate,
            mandatory_conditions=mock_storage.get_schema()
            .get_data_source()
            .get_mandatory_conditions(),
        ),
        selected_columns=[
            SelectedExpression(
                'tags["something"]',
                f.arrayElement(
                    column("tags.value"),
                    f.indexOf(column("tags.key"), "something"),
                    alias="_snuba_tags[something]",
                ),
            )
        ],
        condition=equals(literal(1), literal(1)),
    )
    assert repr(result.data) == repr(expected)
    assert result.data == expected
