from base import BaseTest

from snuba import cleanup, settings
from snuba.clickhouse import Clickhouse


class TestCleanup(BaseTest):
    def setup_method(self, test_method):
        super(TestCleanup, self).setup_method(test_method)

        self.clickhouse = Clickhouse('localhost')

    def test_blank(self):
        assert cleanup.get_active_partitions(self.clickhouse, 'default', 'missing') == []
