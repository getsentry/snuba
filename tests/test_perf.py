from base import BaseTest

from snuba import perf


class TestPerf(BaseTest):
    def test(self):
        assert self.clickhouse.execute("SELECT COUNT() FROM %s" % self.table)[0][0] == 0

        perf.run('tests/perf-event.json', self.clickhouse, self.table)

        assert self.clickhouse.execute("SELECT COUNT() FROM %s" % self.table)[0][0] == 1
