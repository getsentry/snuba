import queue
from datetime import datetime, timedelta
from unittest import mock

import pytest
from clickhouse_driver import errors
from dateutil.tz import tz

from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.native import ClickhousePool, transform_datetime


def test_transform_datetime() -> None:
    now = datetime(2020, 1, 2, 3, 4, 5)
    fmt = "2020-01-02T03:04:05+00:00"
    assert transform_datetime(now) == fmt
    assert transform_datetime(now.replace(tzinfo=tz.tzutc())) == fmt

    offset = timedelta(hours=8)
    assert (
        transform_datetime(now.replace(tzinfo=tz.tzoffset("PST", offset)) + offset)
        == fmt
    )


def test_concurrency_limit() -> None:
    connection = mock.Mock()
    connection.execute.side_effect = ClickhouseError(
        "some error", extra_data={"code": 1}
    )

    pool = ClickhousePool("host", 100, "test", "test", "test")
    pool.pool = queue.LifoQueue(1)
    pool.pool.put(connection, block=False)

    with pytest.raises(ClickhouseError):
        pool.execute_robust("SELECT something")
    connection.execute.assert_called_once()

    connection.reset_mock(side_effect=True)
    connection.execute.side_effect = ClickhouseError(
        "some error", code=errors.ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES,
    )

    with pytest.raises(ClickhouseError):
        pool.execute_robust("SELECT something")
    assert connection.execute.call_count == 3, "Expected three attempts"
