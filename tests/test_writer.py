from base import BaseTest

from snuba.settings import WRITER_COLUMNS
from snuba.processor import process_raw_event
from snuba.writer import row_from_processed_event


class TestWriter(BaseTest):
    def test(self):
        self.write_raw_events(self.event)

        res = self.conn.execute("SELECT count() FROM %s" % self.table)

        assert res[0][0] == 1

    def test_columns_match_schema(self):
        processed = process_raw_event(self.event)
        row = row_from_processed_event(processed, WRITER_COLUMNS)

        assert len(processed) == len(row)

    def test_unknown_columns(self):
        """Fields in a processed events are ignored if they don't have
        a corresponding Clickhouse column declared."""

        processed = process_raw_event(self.event)

        assert 'sdk_name' in processed
        sdk_name = processed['sdk_name']

        assert 'sdk_name' in WRITER_COLUMNS
        columns_copy = WRITER_COLUMNS[:]
        columns_copy.remove('sdk_name')

        row = row_from_processed_event(processed, columns_copy)

        assert len(row) == len(columns_copy)
        assert sdk_name not in row
