import pytest

from typing import Iterable

from tests.base import BaseEventsTest
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.http import HTTPBatchWriter
from snuba.datasets.factory import enforce_table_writer
from snuba.writer import WriterTableRow


class FakeHTTPWriter(HTTPBatchWriter):
    def chunk(self, rows: Iterable[WriterTableRow]) -> Iterable[bytes]:
        return self._prepare_chunks(rows)


class TestHTTPBatchWriter(BaseEventsTest):
    def test_error_handling(self):
        try:
            enforce_table_writer(self.dataset).get_writer(table_name="invalid").write(
                [{"x": "y"}]
            )
        except ClickhouseError as error:
            assert error.code == 60
        else:
            assert False, "expected error"

        try:
            enforce_table_writer(self.dataset).get_writer().write(
                [{"timestamp": "invalid"}]
            )
        except ClickhouseError as error:
            assert error.code == 41
        else:
            assert False, "expected error"

    test_data = [
        (1, [b"a", b"b", b"c"], [b"a", b"b", b"c"],),
        (0, [b"a", b"b", b"c"], [b"abc"],),
        (2, [b"a", b"b", b"c"], [b"ab", b"c"],),
        (2, [b"a", b"b", b"c", b"d"], [b"ab", b"cd"],),
        (100000, [b"a", b"b", b"c"], [b"abc"],),
        (5, [], [],),
    ]

    @pytest.mark.parametrize("chunk_size, input, expected_chunks", test_data)
    def test_chunks(self, chunk_size, input, expected_chunks):
        writer = FakeHTTPWriter(
            "mysterious_inexistent_table",
            "0:0:0:0",
            9000,
            "default",
            "",
            "default",
            lambda a: a,
            None,
            chunk_size,
        )
        chunks = writer.chunk(input)
        for chunk, expected in zip(chunks, expected_chunks):
            assert chunk == expected
