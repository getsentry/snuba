from base import BaseTest

from snuba.datasets.factory import get_dataset
from snuba import perf


class TestPerf(BaseTest):
    def test(self):
        assert self.clickhouse.execute("SELECT COUNT() FROM %s" % self.table)[0][0] == 0

        dataset = get_dataset('events')
        perf.run('tests/perf-event.json', self.clickhouse, dataset)

        assert self.clickhouse.execute("SELECT COUNT() FROM %s" % self.table)[0][0] == 1
