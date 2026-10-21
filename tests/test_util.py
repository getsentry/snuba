import time
from collections.abc import Generator
from datetime import UTC, datetime

import pytest

from snuba.clickhouse.escaping import escape_alias, escape_identifier
from snuba.util import PartSegment, decode_part_str


def test_escape_identifier() -> None:
    assert escape_identifier(None) is None
    assert escape_identifier("") == ""
    assert escape_identifier("foo") == "foo"
    assert escape_identifier("foo.bar") == "foo.bar"
    assert escape_identifier("foo:bar") == "`foo:bar`"

    # Even though backtick characters in columns should be
    # disallowed by the query schema, make sure we dont allow
    # injection anyway.
    assert escape_identifier("`") == r"`\``"
    assert escape_identifier("production`; --") == r"`production\`; --`"


def test_escape_alias() -> None:
    assert escape_alias(None) is None
    assert escape_alias("") == ""
    assert escape_alias("foo") == "foo"
    assert escape_alias("foo.bar") == "`foo.bar`"
    assert escape_alias("foo:bar") == "`foo:bar`"


@pytest.fixture
def local_timezone(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
    # A zone east of UTC, where local midnight falls on the previous UTC day.
    with monkeypatch.context() as m:
        m.setenv("TZ", "Asia/Tokyo")
        time.tzset()
        yield
    time.tzset()


@pytest.mark.usefixtures("local_timezone")
def test_decode_part_str_ignores_local_timezone() -> None:
    part = decode_part_str("(90,'2026-10-12')", [PartSegment.RETENTION_DAYS, PartSegment.DATE])
    assert part.date == datetime(2026, 10, 12, tzinfo=UTC)
    assert part.date.weekday() == 0
    assert part.retention_days == 90
