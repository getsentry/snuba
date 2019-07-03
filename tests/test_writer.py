from base import BaseEventsTest

from snuba.clickhouse import ColumnSet


class TestWriter(BaseEventsTest):
    def test(self):
        self.write_raw_events(self.event)

        res = self.clickhouse.execute("SELECT count() FROM %s" % self.table)

        assert res[0][0] == 1

    def test_columns_match_schema(self):
        _, processed = self.dataset.get_processor().process_message(self.event)
        row = self.dataset.row_from_processed_message(processed)

        # verify that the 'count of columns from event' + 'count of columns from metadata'
        # equals the 'count of columns' in the processed row tuple
        # note that the content is verified in processor tests
        assert (len(processed) + len(self.dataset.get_metadata_columns())) == len(row)

    def test_unknown_columns(self):
        """Fields in a processed events are ignored if they don't have
        a corresponding Clickhouse column declared."""

        _, processed = self.dataset.get_processor().process_message(self.event)

        processed['unknown_field'] = "unknown_value"
        row = self.dataset.row_from_processed_message(processed)

        assert len(row) == len(self.dataset.get_schema().get_columns())
        assert "sdk_name" not in row
