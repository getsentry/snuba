from base import BaseTest

from snuba.clickhouse import ColumnSet, ALL_COLUMNS, METADATA_COLUMNS
from snuba.processor import process_message
from snuba.writer import row_from_processed_event


class TestWriter(BaseTest):
    def test(self):
        self.write_raw_events(self.event)

        res = self.clickhouse.execute("SELECT count() FROM %s" % self.dataset.SCHEMA.QUERY_TABLE)

        assert res[0][0] == 1

    def test_columns_match_schema(self):
        _, processed = process_message(self.event)
        row = row_from_processed_event(self.dataset, processed)

        # verify that the 'count of columns from event' + 'count of columns from metadata'
        # equals the 'count of columns' in the processed row tuple
        # note that the content is verified in processor tests
        assert (len(processed) + len(METADATA_COLUMNS)) == len(row)

    def test_unknown_columns(self):
        """Fields in a processed events are ignored if they don't have
        a corresponding Clickhouse column declared."""

        _, processed = process_message(self.event)
        processed['bad_column'] = "the schema doesn't know about me"

        row = row_from_processed_event(self.dataset, processed)

        assert len(row) == len(self.dataset.SCHEMA.ALL_COLUMNS.columns)
        assert processed['bad_column'] not in row
