import gzip
from typing import Optional

import pytest
import rapidjson

from snuba.clickhouse.errors import ClickhouseWriterError
from snuba.clickhouse.formatter.nodes import FormattedQuery
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend


class TestHTTPBatchWriter:
    storage = get_writable_storage(StorageKey.ERRORS)
    metrics = DummyMetricsBackend(strict=True)

    def test_empty_batch(self) -> None:
        self.storage.get_table_writer().get_batch_writer(metrics=self.metrics).write([])

    def test_error_handling(self) -> None:
        table_writer = self.storage.get_table_writer()

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


DATA = """project_id,id,status,last_seen,first_seen,active_at,first_release_id
2,1409156,0,2021-03-13 00:43:02,2021-03-13 00:43:02,2021-03-13 00:43:02,
2,1409157,0,2021-03-13 00:43:02,2021-03-13 00:43:02,2021-03-13 00:43:02,
"""


class FakeQuery(FormattedQuery):
    def get_sql(self, format: Optional[str] = None) -> str:
        return "SELECT count() FROM groupedmessage_local;"


def test_gzip_load() -> None:
    content = gzip.compress(DATA.encode("utf-8"))

    storage = get_writable_storage(StorageKey.GROUPEDMESSAGES)
    metrics = DummyMetricsBackend(strict=True)
    writer = storage.get_table_writer().get_bulk_writer(
        metrics,
        "gzip",
        [
            "project_id",
            "id",
            "status",
            "last_seen",
            "first_seen",
            "active_at",
            "first_release_id",
        ],
        options=None,
        table_name="groupedmessage_local",
    )

    writer.write([content])

    reader = storage.get_cluster().get_reader()

    ret = reader.execute(FakeQuery([]))
    assert ret["data"][0] == {"count()": 2}
