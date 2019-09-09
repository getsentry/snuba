import pytest

from typing import Iterable

from base import BaseEventsTest
from snuba.clickhouse.http import ClickHouseError, HTTPBatchWriter
from snuba import settings
from snuba.writer import WriterTableRow


class FakeHTTPWriter(HTTPBatchWriter):
    def chunk(self, rows: Iterable[WriterTableRow]) -> Iterable[bytes]:
        return self._prepare_body(rows)


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

    test_data = [
        (
            1,
            ["a", "b", "c"],
            [b"a", b"b", b"c"],
        ),
        (
            None,
            ["a", "b", "c"],
            [b"a", b"b", b"c"],
        ),
        (
            2,
            ["a", "b", "c"],
            [b"ab", b"c"],
        ),
        (
            2,
            ["a", "b", "c", "d"],
            [b"ab", b"cd"],
        ),
        (
            100000,
            ["a", "b", "c"],
            [b"abc"],
        ),
        (
            5,
            [],
            [],
        )
    ]

    @pytest.mark.parametrize("chunk_size, input, expected_chunks", test_data)
    def test_chunks(self, chunk_size, input, expected_chunks):
        writer = FakeHTTPWriter(
            None,
            settings.CLICKHOUSE_HOST,
            settings.CLICKHOUSE_HTTP_PORT,
            lambda a: a,
            None,
            "mysterious_inexistent_table",
            chunk_size
        )
        chunks = writer.chunk(input)
        for chunk, expected in zip(chunks, expected_chunks):
            assert chunk == expected
