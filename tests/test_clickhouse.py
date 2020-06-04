from unittest.mock import call, patch

from clickhouse_driver import errors

from snuba.clickhouse.columns import Array, ColumnSet, Nested, Nullable, String, UInt
from snuba.clickhouse.native import ClickhousePool
from snuba.datasets.factory import enforce_table_writer
from tests.base import BaseEventsTest


class TestClickhouse(BaseEventsTest):
    def test_flattened(self):
        columns = enforce_table_writer(self.dataset).get_schema().get_columns()
        assert columns["group_id"].type == UInt(64)
        assert columns["group_id"].name == "group_id"
        assert columns["group_id"].base_name is None
        assert columns["group_id"].flattened == "group_id"

        assert columns["exception_frames.in_app"].type == Array(Nullable(UInt(8)))
        assert columns["exception_frames.in_app"].name == "in_app"
        assert columns["exception_frames.in_app"].base_name == "exception_frames"
        assert columns["exception_frames.in_app"].flattened == "exception_frames.in_app"

    def test_schema(self):
        cols = ColumnSet([("foo", UInt(8)), ("bar", Nested([("qux:mux", String())]))])

        assert cols.for_schema() == "foo UInt8, bar Nested(`qux:mux` String)"
        assert cols["foo"].type == UInt(8)
        assert cols["bar.qux:mux"].type == Array(String())

    @patch("snuba.clickhouse.native.Client")
    def test_reconnect(self, FakeClient):
        # If the connection NetworkErrors a first time, make sure we call it a second time.
        FakeClient.return_value.execute.side_effect = [
            errors.NetworkError,
            '{"data": "to my face"}',
        ]
        cp = ClickhousePool("0:0:0:0", 9000, "default", "", "default")
        cp.execute("SHOW TABLES")
        assert FakeClient.return_value.execute.mock_calls == [
            call("SHOW TABLES"),
            call("SHOW TABLES"),
        ]
