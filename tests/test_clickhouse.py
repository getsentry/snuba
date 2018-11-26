from base import BaseTest

from snuba.clickhouse import (
    ALL_COLUMNS,
    Array, ColumnSet, Nested, Nullable, String, UInt,
    escape_col
)


class TestClickhouse(BaseTest):
    def test_escape_col(self):
        assert escape_col(None) is None
        assert escape_col('') == ''
        assert escape_col('foo') == 'foo'
        assert escape_col('foo.bar') == 'foo.bar'
        assert escape_col('foo:bar') == '`foo:bar`'

    def test_flattened(self):
        assert ALL_COLUMNS['group_id'].type == UInt(64)
        assert ALL_COLUMNS['group_id'].name == 'group_id'
        assert ALL_COLUMNS['group_id'].base_name is None
        assert ALL_COLUMNS['group_id'].flattened == 'group_id'

        assert ALL_COLUMNS['exception_frames.in_app'].type == Array(Nullable(UInt(8)))
        assert ALL_COLUMNS['exception_frames.in_app'].name == 'in_app'
        assert ALL_COLUMNS['exception_frames.in_app'].base_name == 'exception_frames'
        assert ALL_COLUMNS['exception_frames.in_app'].flattened == 'exception_frames.in_app'

    def test_schema(self):
        cols = ColumnSet([
            ('foo', UInt(8)),
            ('bar', Nested([
                ('qux:mux', String())
            ]))
        ])

        assert cols.for_schema() == 'foo UInt8, bar Nested(`qux:mux` String)'
        assert cols['foo'].type == UInt(8)
        assert cols['bar.qux:mux'].type == Array(String())
