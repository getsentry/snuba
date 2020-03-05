from datetime import datetime, timedelta
from dateutil.tz import tz
from snuba.clickhouse.native import transform_datetime


def test_transform_datetime() -> None:
    now = datetime(2020, 1, 2, 3, 4, 5)
    fmt = "2020-01-02T03:04:05+00:00"
    assert transform_datetime(now) == fmt
    assert transform_datetime(now.replace(tzinfo=tz.tzutc())) == fmt
    assert (
        transform_datetime(now.replace(tzinfo=tz.gettz("PST")) - timedelta(hours=8))
        == fmt
    )
