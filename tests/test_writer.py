import gzip
from typing import Optional

import pytest
import rapidjson

from snuba.clickhouse.errors import ClickhouseWriterError
from snuba.clickhouse.formatter.nodes import FormattedQuery
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import enforce_table_writer, get_entity
from snuba.datasets.factory import get_dataset
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend


class TestHTTPBatchWriter:
    dataset = get_dataset("events")
    entity = get_entity(EntityKey.EVENTS)
    metrics = DummyMetricsBackend(strict=True)

    @pytest.mark.clickhouse_db
    def test_empty_batch(self) -> None:
        enforce_table_writer(self.entity).get_batch_writer(metrics=self.metrics).write(
            []
        )

    @pytest.mark.clickhouse_db
    def test_error_handling(self) -> None:
        table_writer = enforce_table_writer(self.entity)

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


@pytest.mark.clickhouse_db
def test_gzip_load() -> None:
    content = gzip.compress(DATA.encode("utf-8"))

    entity = get_entity(EntityKey.GROUPEDMESSAGE)
    metrics = DummyMetricsBackend(strict=True)
    writer = enforce_table_writer(entity).get_bulk_writer(
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

    cluster = entity.get_all_storages()[0].get_cluster()
    reader = cluster.get_reader()

    ret = reader.execute(FakeQuery([]))
    assert ret["data"][0] == {"count()": 2}
