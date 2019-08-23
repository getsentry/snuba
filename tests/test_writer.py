from base import BaseEventsTest
from snuba.writer import ClickHouseError, HTTPBatchWriter


class TestHTTPBatchWriter(BaseEventsTest):
    def test_error_handling(self):
        try:
            self.dataset.get_writer(table_name="invalid").write([{"x": "y"}])
        except ClickHouseError as error:
            assert error.code == 60
            assert error.type == 'DB::Exception'
        else:
            assert False, "expected error"

        try:
            self.dataset.get_writer().write([{"timestamp": "invalid"}])
        except ClickHouseError as error:
            assert error.code == 41
            assert error.type == 'DB::Exception'
        else:
            assert False, "expected error"
