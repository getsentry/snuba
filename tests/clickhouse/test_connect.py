from typing import Any, cast
from unittest import mock

import pytest

from snuba.clickhouse.connect import ClickhouseConnectPool, _is_explain_query
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.formatter.nodes import FormattedQuery

# Error code returned by ClickHouse when the maximum number of simultaneous
# queries has been exceeded.
TOO_MANY_SIMULTANEOUS_QUERIES = 202


class FakeColumnType:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeQueryResult:
    def __init__(
        self,
        result_set: Any,
        column_names: Any = (),
        column_types: Any = (),
        summary: Any = None,
    ) -> None:
        self.result_set = result_set
        self.column_names = column_names
        self.column_types = column_types
        self.summary = summary or {}


def _make_pool(client: mock.Mock) -> ClickhouseConnectPool:
    pool = ClickhouseConnectPool(
        host="host",
        user="test",
        password="test",
        database="test",
    )
    # Avoid creating a real client / connection.
    pool._get_client = lambda: client  # type: ignore[method-assign]
    return pool


def test_execute_maps_result_and_profile() -> None:
    client = mock.Mock()
    client.query.return_value = FakeQueryResult(
        result_set=[[1, "a"], [2, "b"]],
        column_names=("id", "name"),
        column_types=(FakeColumnType("UInt64"), FakeColumnType("String")),
        summary={"read_rows": "2", "read_bytes": "128", "elapsed_ns": "1500000000"},
    )

    pool = _make_pool(client)
    result = pool.execute("SELECT id, name FROM t", with_column_types=True)

    assert result.results == [[1, "a"], [2, "b"]]
    assert result.meta == [("id", "UInt64"), ("name", "String")]
    assert result.profile is not None
    assert result.profile["rows"] == 2
    assert result.profile["bytes"] == 128
    assert result.profile["elapsed"] == 1.5


def test_execute_passes_query_id_and_settings() -> None:
    client = mock.Mock()
    client.query.return_value = FakeQueryResult(result_set=[])

    pool = _make_pool(client)
    pool.execute(
        "SELECT 1",
        query_id="my-query-id",
        settings={"max_threads": 4},
    )

    _, kwargs = client.query.call_args
    assert kwargs["settings"]["query_id"] == "my-query-id"
    assert kwargs["settings"]["max_threads"] == 4


def test_too_many_simultaneous_queries_not_retried() -> None:
    # We delegate all retries to clickhouse-connect, which does not retry the
    # TOO_MANY_SIMULTANEOUS_QUERIES error. It should be surfaced directly,
    # mapped to a ClickhouseError that preserves the error code.
    from clickhouse_connect.driver.exceptions import DatabaseError

    client = mock.Mock()
    client.query.side_effect = DatabaseError("too many", code=TOO_MANY_SIMULTANEOUS_QUERIES)

    pool = _make_pool(client)
    try:
        pool.execute("SELECT 1")
        raise AssertionError("expected a ClickhouseError to be raised")
    except ClickhouseError as error:
        assert error.code == TOO_MANY_SIMULTANEOUS_QUERIES
    # No retry on top of clickhouse-connect's own handling.
    assert client.query.call_count == 1


def test_operational_error_mapped_without_extra_retries() -> None:
    # Connection-level retries are clickhouse-connect's responsibility; we only
    # map the surfaced error onto ClickhouseError without retrying again.
    from clickhouse_connect.driver.exceptions import OperationalError

    client = mock.Mock()
    client.query.side_effect = OperationalError("connection refused")

    pool = _make_pool(client)
    with pytest.raises(ClickhouseError):
        pool.execute("SELECT 1", retryable=True)

    assert client.query.call_count == 1


def test_generic_clickhouse_error_wrapped() -> None:
    # Any clickhouse-connect error (here a ProgrammingError) must be wrapped in
    # a snuba ClickhouseError, matching how the native pool wraps the whole
    # clickhouse_driver errors.Error family.
    from clickhouse_connect.driver.exceptions import ProgrammingError

    client = mock.Mock()
    client.query.side_effect = ProgrammingError("bad query")

    pool = _make_pool(client)
    with pytest.raises(ClickhouseError):
        pool.execute("SELECT 1")

    assert client.query.call_count == 1


def test_timeouts_are_passed_through() -> None:
    import clickhouse_connect

    # The per-profile timeout is honored as-is (no capping), the same way the
    # native driver uses it. A large timeout (e.g. from MIGRATE) is preserved.
    pool = ClickhouseConnectPool(
        host="host",
        user="test",
        password="test",
        database="test",
        connect_timeout=60,
        send_receive_timeout=300000,
    )

    with (
        mock.patch.object(clickhouse_connect, "get_client") as get_client,
        mock.patch("snuba.clickhouse.connect.get_pool_manager"),
    ):
        pool._get_client()

    _, kwargs = get_client.call_args
    assert kwargs["send_receive_timeout"] == 300000
    assert kwargs["connect_timeout"] == 60


def test_send_receive_timeout_unbounded_when_profile_has_none() -> None:
    import clickhouse_connect

    from snuba.clickhouse.connect import UNBOUNDED_SEND_RECEIVE_TIMEOUT_SECONDS

    # A profile with no timeout (None) means "unbounded" on the native path; over
    # HTTP that maps to a large finite timeout, since clickhouse-connect can't
    # take None.
    pool = ClickhouseConnectPool(
        host="host",
        user="test",
        password="test",
        database="test",
        send_receive_timeout=None,
    )

    with (
        mock.patch.object(clickhouse_connect, "get_client") as get_client,
        mock.patch("snuba.clickhouse.connect.get_pool_manager"),
    ):
        pool._get_client()

    _, kwargs = get_client.call_args
    assert kwargs["send_receive_timeout"] == UNBOUNDED_SEND_RECEIVE_TIMEOUT_SECONDS


def test_read_query_client_settings_use_25s_timeout() -> None:
    # Read queries (the QUERY profile) get a 25s timeout on both drivers, leaving
    # headroom under the frontend request budget.
    from snuba.clusters.cluster import ClickhouseClientSettings

    assert ClickhouseClientSettings.QUERY.value.timeout == 25


def test_internal_profile_is_unbounded() -> None:
    # The 30s read timeout must not leak onto internal/maintenance queries
    # (topology discovery, routing load lookups, delete throttling checks, the
    # span-export job, table copies). They use the INTERNAL profile, which stays
    # unbounded so long-running operations aren't capped at 30s.
    from snuba.clusters.cluster import ClickhouseClientSettings

    assert ClickhouseClientSettings.INTERNAL.value.timeout is None


def test_pool_size_defaults_to_setting() -> None:
    import clickhouse_connect

    from snuba import settings

    pool = ClickhouseConnectPool(host="host", user="test", password="test", database="test")

    with (
        mock.patch.object(clickhouse_connect, "get_client"),
        mock.patch("snuba.clickhouse.connect.get_pool_manager") as get_pool_manager,
    ):
        pool._get_client()

    _, kwargs = get_pool_manager.call_args
    assert kwargs["maxsize"] == settings.CLICKHOUSE_MAX_POOL_SIZE


@pytest.mark.redis_db
def test_pool_size_runtime_override() -> None:
    import clickhouse_connect

    from snuba import state

    state.set_config("clickhouse_connect_pool_size", 42)

    pool = ClickhouseConnectPool(host="host", user="test", password="test", database="test")

    with (
        mock.patch.object(clickhouse_connect, "get_client"),
        mock.patch("snuba.clickhouse.connect.get_pool_manager") as get_pool_manager,
    ):
        pool._get_client()

    _, kwargs = get_pool_manager.call_args
    assert kwargs["maxsize"] == 42


def test_clickhouse_reader_wraps_connect_pool() -> None:
    # The single driver-agnostic ClickhouseReader wraps the abstract pool, so it
    # works with the connect pool just like the native one.
    from snuba.clickhouse.native import ClickhouseReader

    pool = _make_pool(mock.Mock())
    reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)
    assert isinstance(reader, ClickhouseReader)


def test_with_totals_handled_over_http() -> None:
    # WITH TOTALS works on the HTTP driver through clickhouse-connect's own
    # parsing: its Native-format reader concatenates every response block,
    # including the trailing totals block, into result_set, so the totals row
    # arrives last — exactly what the driver-agnostic ClickhouseReader expects
    # when it pops the last row as "totals". No native-driver-specific handling
    # is involved.
    from snuba.clickhouse.native import ClickhouseReader

    class FakeFormattedQuery:
        def get_sql(self) -> str:
            return "SELECT project_id, count() FROM t GROUP BY project_id WITH TOTALS"

    client = mock.Mock()
    # Two data rows followed by the totals row, the way clickhouse-connect
    # surfaces a WITH TOTALS response over HTTP.
    client.query.return_value = FakeQueryResult(
        result_set=[[1, 10], [2, 20], [0, 30]],
        column_names=("project_id", "count()"),
        column_types=(FakeColumnType("UInt64"), FakeColumnType("UInt64")),
    )

    pool = _make_pool(client)
    reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)

    result = reader.execute(cast(FormattedQuery, FakeFormattedQuery()), with_totals=True)

    # The trailing row is split out as totals; only the real rows remain in data.
    assert result["data"] == [
        {"project_id": 1, "count()": 10},
        {"project_id": 2, "count()": 20},
    ]
    assert result["totals"] == {"project_id": 0, "count()": 30}


def test_connect_type_names_drive_reader_transforms() -> None:
    # The connect pool exposes clickhouse-connect's column_type.name in the
    # result meta, and the driver-agnostic ClickhouseReader runs that through
    # the same Date / DateTime / UUID regex transforms used for the native
    # driver. This pins that clickhouse-connect's type-name format matches what
    # those regexes expect (including parametrized types like DateTime('UTC')
    # and Nullable(UUID)), so transformations are not silently skipped on the
    # HTTP path.
    from datetime import date, datetime
    from uuid import UUID as UUIDClass

    from clickhouse_connect.datatypes.registry import get_from_name

    from snuba.clickhouse.native import ClickhouseReader

    class FakeFormattedQuery:
        def get_sql(self) -> str:
            return "SELECT d, dt, dt_tz, uid, nuid"

    # Column types named exactly as clickhouse-connect produces them. The
    # assertion documents that clickhouse-connect uses the canonical ClickHouse
    # type strings (the same the native driver returns), so the reader regexes
    # match identically for both drivers.
    col_types = [
        get_from_name("Date"),
        get_from_name("DateTime"),
        get_from_name("DateTime('UTC')"),
        get_from_name("UUID"),
        get_from_name("Nullable(UUID)"),
    ]
    assert [c.name for c in col_types] == [
        "Date",
        "DateTime",
        "DateTime('UTC')",
        "UUID",
        "Nullable(UUID)",
    ]

    d = date(2023, 1, 2)
    dt = datetime(2023, 1, 2, 3, 4, 5)
    uid = UUIDClass("00000000-0000-0000-0000-000000000001")

    client = mock.Mock()
    client.query.return_value = FakeQueryResult(
        result_set=[[d, dt, dt, uid, uid]],
        column_names=("d", "dt", "dt_tz", "uid", "nuid"),
        column_types=tuple(col_types),
    )

    pool = _make_pool(client)
    reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)
    result = reader.execute(cast(FormattedQuery, FakeFormattedQuery()))

    # Date/DateTime (incl. the parametrized tz variant) become ISO strings and
    # UUID (incl. Nullable) becomes a string. Had the regexes failed to match
    # the connect type names, these would still be the original objects.
    row = result["data"][0]
    assert row["d"] == "2023-01-02T00:00:00+00:00"
    assert row["dt"] == "2023-01-02T03:04:05+00:00"
    assert row["dt_tz"] == "2023-01-02T03:04:05+00:00"
    assert row["uid"] == "00000000-0000-0000-0000-000000000001"
    assert row["nuid"] == "00000000-0000-0000-0000-000000000001"


def test_explain_query_uses_command_not_native_query() -> None:
    # Regression: EXPLAIN statements must NOT go through clickhouse-connect's
    # Native query() path. query() appends "FORMAT Native", which is swallowed
    # by the inner query being explained, so the EXPLAIN output comes back as
    # text and the Native reader misparses it -- the snuba-admin "Unrecognized
    # ClickHouse type base: essionList ..." failure (a fragment of the EXPLAIN
    # AST dump read as a column type). EXPLAIN must instead use command(), which
    # sends the statement verbatim (no FORMAT appended) and returns decoded text.
    client = mock.Mock()
    ast_dump = (
        "SelectWithUnionQuery (children 1)\n"
        " ExpressionList (children 1)\n"
        "  Identifier query\n"
        " TablesInSelectQuery (children 1)"
    )
    # command() strips the trailing newline and (no tabs) returns the whole dump
    # as a single string.
    client.command.return_value = ast_dump

    pool = _make_pool(client)
    result = pool.execute("EXPLAIN AST SELECT query FROM system.clusters", with_column_types=True)

    # Used the text command() path, never the Native query() path.
    client.command.assert_called_once()
    client.query.assert_not_called()

    # One single-column row per explain line, with indentation preserved --
    # exactly the shape the native driver returns for the same EXPLAIN.
    assert result.results == [
        ("SelectWithUnionQuery (children 1)",),
        (" ExpressionList (children 1)",),
        ("  Identifier query",),
        (" TablesInSelectQuery (children 1)",),
    ]
    assert result.meta == [("explain", "String")]


def test_non_explain_query_uses_native_query_path() -> None:
    # The EXPLAIN special-casing must not divert ordinary queries off the
    # Native query() path.
    client = mock.Mock()
    client.query.return_value = FakeQueryResult(
        result_set=[[1]],
        column_names=("x",),
        column_types=(FakeColumnType("UInt8"),),
    )

    pool = _make_pool(client)
    pool.execute("SELECT 1", with_column_types=True)

    client.query.assert_called_once()
    client.command.assert_not_called()


@pytest.mark.parametrize(
    "query, is_explain",
    [
        ("EXPLAIN AST SELECT 1", True),
        ("explain query tree SELECT 1", True),
        ("  \n EXPLAIN PLAN SELECT 1", True),
        ("SELECT 1", False),
        ("SELECT 'EXPLAIN'", False),  # EXPLAIN only as a string literal
        ("WITH x AS (SELECT 1) SELECT * FROM x", False),
    ],
)
def test_is_explain_query_detection(query: str, is_explain: bool) -> None:
    assert _is_explain_query(query) is is_explain


def test_explain_empty_output_returns_no_rows() -> None:
    # An empty response body makes command() return a QuerySummary (here stubbed
    # by a bare Mock, which is neither str/int/list): that must yield zero rows,
    # not a single empty-string row.
    client = mock.Mock()
    client.command.return_value = mock.Mock()  # stand-in for QuerySummary

    pool = _make_pool(client)
    result = pool.execute("EXPLAIN AST SELECT 1", with_column_types=True)

    assert result.results == []
    assert result.meta == [("explain", "String")]


def test_explain_error_wrapped_and_preserves_code() -> None:
    # Errors from the command() path must be wrapped in a snuba ClickhouseError
    # with the server code preserved -- the system-query validator relies on the
    # code (e.g. UNKNOWN_TABLE) to turn a failed EXPLAIN into a clean rejection.
    from clickhouse_connect.driver.exceptions import DatabaseError

    UNKNOWN_TABLE = 60
    client = mock.Mock()
    client.command.side_effect = DatabaseError("no such table", code=UNKNOWN_TABLE)

    pool = _make_pool(client)
    try:
        pool.execute("EXPLAIN AST SELECT * FROM nope")
        raise AssertionError("expected a ClickhouseError to be raised")
    except ClickhouseError as error:
        assert error.code == UNKNOWN_TABLE


def test_explain_forwards_query_id_and_trace_settings() -> None:
    # query_id and capture_trace must not be silently dropped on the EXPLAIN
    # path: they are folded into the settings handed to command() (via
    # _build_query_settings), the same mapping the Native query() path receives.
    # clickhouse-connect treats query_id as a transport setting and routes it to
    # an HTTP param, so this keeps both driver paths consistent.
    client = mock.Mock()
    client.command.return_value = "ExpressionList (children 1)"

    pool = _make_pool(client)
    pool.execute(
        "EXPLAIN AST SELECT 1",
        query_id="qid-123",
        capture_trace=True,
    )

    _, kwargs = client.command.call_args
    assert kwargs["settings"]["query_id"] == "qid-123"
    assert kwargs["settings"]["send_logs_level"] == "trace"
