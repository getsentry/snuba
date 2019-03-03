from base import BaseTest

from snuba.clickhouse import ColumnSet


class TestWriter(BaseTest):
    def test(self):
        self.write_raw_events(self.event)

        res = self.clickhouse.execute(
            "SELECT count() FROM %s" % self.dataset.SCHEMA.QUERY_TABLE
        )

        assert res[0][0] == 1

    def test_columns_match_schema(self):
        row = self.dataset.PROCESSOR.process_insert(self.event)

        # verify that the 'count of columns from event' + 'count of columns from metadata'
        # equals the 'count of columns' in the processed row tuple
        # note that the content is verified in processor tests
        assert len(self.dataset.SCHEMA.ALL_COLUMNS) == len(row)
