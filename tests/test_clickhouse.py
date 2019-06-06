from base import BaseTest

from clickhouse_driver import Client, errors
from mock import patch, call, Mock

from snuba.clickhouse import (
    Array, ColumnSet, Nested, Nullable, String, UInt,
    escape_col,
    ClickhousePool
)


class TestClickhouse(BaseTest):
    def test_escape_col(self):
        assert escape_col(None) is None
        assert escape_col('') == ''
        assert escape_col('foo') == 'foo'
        assert escape_col('foo.bar') == 'foo.bar'
        assert escape_col('foo:bar') == '`foo:bar`'

        # Even though backtick characters in columns should be
        # disallowed by the query schema, make sure we dont allow
        # injection anyway.
        assert escape_col("`") == r"`\``"
        assert escape_col("production`; --") == r"`production\`; --`"

    def test_flattened(self):
        columns = self.dataset.get_schema().get_columns()
        assert columns['group_id'].type == UInt(64)
        assert columns['group_id'].name == 'group_id'
        assert columns['group_id'].base_name is None
        assert columns['group_id'].flattened == 'group_id'

        assert columns['exception_frames.in_app'].type == Array(Nullable(UInt(8)))
        assert columns['exception_frames.in_app'].name == 'in_app'
        assert columns['exception_frames.in_app'].base_name == 'exception_frames'
        assert columns['exception_frames.in_app'].flattened == 'exception_frames.in_app'

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

    @patch('snuba.clickhouse.Client')
    def test_reconnect(self, FakeClient):
        # If the connection NetworkErrors a first time, make sure we call it a second time.
        FakeClient.return_value.execute.side_effect = [errors.NetworkError, '{"data": "to my face"}']
        cp = ClickhousePool()
        cp.execute("SHOW TABLES")
        assert FakeClient.return_value.execute.mock_calls == [call("SHOW TABLES"), call("SHOW TABLES")]
