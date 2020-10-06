from tests.base import BaseEventsTest

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba import perf


class TestPerf(BaseEventsTest):
    def test(self):
        dataset = get_dataset("events")
        storage = dataset.get_entity().get_writable_storage()
        assert storage is not None
        table = storage.get_table_writer().get_schema().get_local_table_name()
        clickhouse = (
            get_storage(StorageKey.EVENTS)
            .get_cluster()
            .get_query_connection(ClickhouseClientSettings.QUERY)
        )

        assert clickhouse.execute("SELECT COUNT() FROM %s" % table)[0][0] == 0

        perf.run("tests/perf-event.json", dataset)

        assert clickhouse.execute("SELECT COUNT() FROM %s" % table)[0][0] == 1
