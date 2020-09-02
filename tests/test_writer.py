import pytest
import rapidjson

from snuba.clickhouse.errors import ClickhouseWriterError
from snuba.datasets.factory import enforce_table_writer
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from tests.base import BaseEventsTest


class TestHTTPBatchWriter(BaseEventsTest):
    def test_error_handling(self) -> None:
        table_writer = enforce_table_writer(self.dataset)
        metrics = DummyMetricsBackend(strict=True)

        with pytest.raises(ClickhouseWriterError) as error:
            table_writer.get_batch_writer(table_name="invalid", metrics=metrics).write(
                [rapidjson.dumps({"x": "y"}).encode("utf-8")]
            )

        assert error.value.code == 60

        with pytest.raises(ClickhouseWriterError) as error:
            table_writer.get_batch_writer(metrics=metrics).write(
                [b"{}", rapidjson.dumps({"timestamp": "invalid"}).encode("utf-8")]
            )

        assert error.value.code == 41
        assert error.value.row == 2
