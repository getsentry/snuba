from tests.base import BaseEventsTest

from snuba.datasets.factory import get_dataset
from snuba.datasets.storages.factory import get_storage
from snuba import perf


class TestPerf(BaseEventsTest):
    def test(self):
        dataset = get_dataset("events")
        table = dataset.get_table_writer().get_schema().get_local_table_name()
        clickhouse = get_storage("events").get_cluster().get_clickhouse_rw()

        assert clickhouse.execute("SELECT COUNT() FROM %s" % table)[0][0] == 0

        perf.run("tests/perf-event.json", dataset)

        assert clickhouse.execute("SELECT COUNT() FROM %s" % table)[0][0] == 1
