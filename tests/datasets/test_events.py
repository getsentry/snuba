from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.events import (
    ErrorsQueryStorageSelector,
    errors_translators,
)
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.request.request_settings import HTTPRequestSettings
from tests.fixtures import get_raw_event
from tests.helpers import write_unprocessed_events


class TestEventsDataset:
    def test_tags_hash_map(self) -> None:
        """
        Adds an event and ensures the tags_hash_map is properly populated
        including escaping.
        """
        self.event = get_raw_event()
        self.event["data"]["tags"].append(["test_tag1", "value1"])
        self.event["data"]["tags"].append(["test_tag=2", "value2"])  # Requires escaping
        storage = get_writable_storage(StorageKey.ERRORS)
        schema = storage.get_schema()
        assert isinstance(schema, TableSchema)
        table_name = schema.get_table_name()
        write_unprocessed_events(storage, [self.event])

        clickhouse = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.QUERY
        )

        hashed = clickhouse.execute(
            "SELECT cityHash64('test_tag1=value1'), cityHash64('test_tag\\\\=2=value2')"
        )
        tag1, tag2 = hashed[0]

        event = clickhouse.execute(
            (
                f"SELECT replaceAll(toString(event_id), '-', '') FROM {table_name} WHERE has(_tags_hash_map, {tag1}) "
                f"AND has(_tags_hash_map, {tag2})"
            )
        )
        assert len(event) == 1
        assert event[0][0] == self.event["data"]["id"]


def test_storage_selector() -> None:
    state.set_config("enable_events_readonly_table", True)

    storage = get_storage(StorageKey.ERRORS)
    storage_ro = get_storage(StorageKey.ERRORS_RO)

    query = Query(Entity(EntityKey.EVENTS, ColumnSet([])), selected_columns=[])

    storage_selector = ErrorsQueryStorageSelector(mappers=errors_translators)
    assert (
        storage_selector.select_storage(
            query, HTTPRequestSettings(consistent=False)
        ).storage
        == storage_ro
    )
    assert (
        storage_selector.select_storage(
            query, HTTPRequestSettings(consistent=True)
        ).storage
        == storage
    )
