from snuba import state
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.events import EventsQueryStorageSelector
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.query.logical import Query
from snuba.request.request_settings import HTTPRequestSettings
from tests.base import BaseEventsTest


class TestEventsDataset(BaseEventsTest):
    def test_tags_hash_map(self) -> None:
        """
        Adds an event and ensures the tags_hash_map is properly populated
        including escaping.
        """

        self.event["data"]["tags"].append(["test_tag1", "value1"])
        self.event["data"]["tags"].append(["test_tag=2", "value2"])  # Requires escaping
        self.write_events([self.event])

        clickhouse = (
            get_storage(StorageKey.EVENTS)
            .get_cluster()
            .get_query_connection(ClickhouseClientSettings.QUERY)
        )

        hashed = clickhouse.execute(
            "SELECT cityHash64('test_tag1=value1'), cityHash64('test_tag\\\\=2=value2')"
        )
        tag1, tag2 = hashed[0]

        event = clickhouse.execute(
            (
                f"SELECT event_id FROM sentry_local WHERE has(_tags_hash_map, {tag1}) "
                f"AND has(_tags_hash_map, {tag2})"
            )
        )
        assert len(event) == 1
        assert event[0][0] == self.event["data"]["id"]


def test_storage_selector() -> None:
    state.set_config("enable_events_readonly_table", True)

    storage = get_storage(StorageKey.EVENTS)
    storage_ro = get_storage(StorageKey.EVENTS_RO)

    query = Query({}, storage.get_schema().get_data_source())

    storage_selector = EventsQueryStorageSelector(storage, storage_ro)
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
