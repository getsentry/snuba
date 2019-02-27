from base import BaseTest

from snuba import perf


class TestPerf(BaseTest):
    def test(self):
        table = self.dataset.SCHEMA.QUERY_TABLE
        assert self.clickhouse.execute("SELECT COUNT() FROM %s" % table)[0][0] == 0

        perf.run('tests/perf-event.json', self.clickhouse, self.dataset)

        assert self.clickhouse.execute("SELECT COUNT() FROM %s" % table)[0][0] == 1
