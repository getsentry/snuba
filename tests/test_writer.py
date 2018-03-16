from base import BaseTest


class TestWriter(BaseTest):
    def test(self):
        self.write_raw_events(self.base_event)

        res = self.conn.execute("SELECT count() FROM %s" % self.table)

        assert res[0][0] == 1
