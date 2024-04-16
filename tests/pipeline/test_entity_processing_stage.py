from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.clusters.cluster import _STORAGE_SET_CLUSTER_MAP, CLUSTERS
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
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_processing import EntityProcessingStage
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.dsl import and_cond, column, equals, literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query_initial
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.utils.schemas import Column as ColumnSchema
from snuba.utils.schemas import DateTime, UInt

"""
Create a fake storage and entity, put the entity in global definitions
"""
entkey = EntityKey("mock_entity")
storkey = StorageKey("mockstorage")
storsetkey = StorageSetKey("mockstorageset")
colset = ColumnSet(
    [
        ColumnSchema("org_id", UInt(64)),
        ColumnSchema("project_id", UInt(64)),
        ColumnSchema("timestamp", DateTime()),
    ]
)
storage = ReadableTableStorage(
    storage_key=storkey,
    storage_set_key=storsetkey,
    schema=TableSchema(
        columns=colset,
        local_table_name=f"{storkey.value}_local",
        dist_table_name=f"{storkey.value}_dist",
        storage_set_key=storsetkey,
    ),
    readiness_state=ReadinessState.COMPLETE,
)
_STORAGE_SET_CLUSTER_MAP[storage.get_storage_set_key()] = CLUSTERS[0]
entity = PluggableEntity(
    entity_key=entkey,
    storages=[EntityStorageConnection(storage, TranslationMappers())],
    query_processors=[],
    columns=colset.columns,
    validators=[],
    required_time_column="timestamp",
    storage_selector=DefaultQueryStorageSelector(),
)
override_entity_map(entkey, entity)


def test_basic() -> None:
    query_body = {
        "query": (
            f"MATCH ({entkey.value}) "
            "SELECT timestamp "
            "WHERE "
            "org_id = 1 AND "
            "project_id = 1"
        ),
    }
    logical_query = parse_snql_query_initial(query_body["query"])
    assert isinstance(logical_query, LogicalQuery)
    query_settings = HTTPQuerySettings()
    timer = Timer("test")
    request = Request(
        id="",
        original_body=query_body,
        query=logical_query,
        snql_anonymized="",
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
    )
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    expected = QueryPipelineResult(
        data=Query(
            from_clause=Table(
                table_name=schema.get_table_name(),
                schema=storage.get_schema().get_columns(),
                storage_key=storkey,
                allocation_policies=storage.get_allocation_policies(),
                final=logical_query.get_final(),
                sampling_rate=logical_query.get_sample(),
                mandatory_conditions=storage.get_schema()
                .get_data_source()
                .get_mandatory_conditions(),
            ),
            selected_columns=[SelectedExpression("timestamp", column("timestamp"))],
            condition=and_cond(
                equals(column("org_id"), literal(1)),
                equals(column("project_id"), literal(1)),
            ),
            limit=1000,
        ),
        query_settings=query_settings,
        timer=timer,
        error=None,
    )
    actual = EntityProcessingStage().execute(
        QueryPipelineResult(
            data=request,
            query_settings=request.query_settings,
            timer=timer,
            error=None,
        )
    )
    assert actual == expected
    reset_entity_factory()
