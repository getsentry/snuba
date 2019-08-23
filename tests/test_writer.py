from base import BaseEventsTest
from snuba.writer import ClickHouseError, HTTPBatchWriter


class TestHTTPBatchWriter(BaseEventsTest):
    def test_error_handling(self):
        try:
            self.dataset.get_writer(table_name="invalid").write([{"x": "y"}])
        except ClickHouseError as error:
            assert error.code == 60
        else:
            assert False, "expected error"
