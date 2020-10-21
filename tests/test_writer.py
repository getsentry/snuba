import pytest
import rapidjson

from snuba.clickhouse.errors import ClickhouseWriterError
from snuba.datasets.factory import enforce_table_writer, get_dataset
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend


class TestHTTPBatchWriter:
    dataset = get_dataset("events")
    metrics = DummyMetricsBackend(strict=True)

    def test_empty_batch(self) -> None:
        enforce_table_writer(self.dataset).get_batch_writer(metrics=self.metrics).write(
            []
        )

    def test_error_handling(self) -> None:
        table_writer = enforce_table_writer(self.dataset)

        with pytest.raises(ClickhouseWriterError) as error:
            table_writer.get_batch_writer(
                table_name="invalid", metrics=self.metrics
            ).write([rapidjson.dumps({"x": "y"}).encode("utf-8")])

        assert error.value.code == 60

        with pytest.raises(ClickhouseWriterError) as error:
            table_writer.get_batch_writer(metrics=self.metrics).write(
                [b"{}", rapidjson.dumps({"timestamp": "invalid"}).encode("utf-8")]
            )

        assert error.value.code == 41
        assert error.value.row == 2
