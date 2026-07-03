import json
import uuid
from datetime import date, datetime
from typing import Any, cast
from unittest import mock

import pytest

from snuba.clickhouse.connect import ClickhouseConnectPool
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


def test_totals_malformed_json_wrapped() -> None:
    # A non-JSON totals response (truncation, a proxy error page) surfaces as a
    # ClickhouseError rather than leaking a raw JSONDecodeError.
    client = mock.Mock()
    client.raw_query.return_value = b"<html>502 Bad Gateway</html>"

    pool = _make_pool(client)
    with pytest.raises(ClickhouseError):
        pool.execute_with_totals("SELECT g, sum(v) FROM t GROUP BY g WITH TOTALS")


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


def test_with_totals_via_single_jsoncompact_request() -> None:
    # WITH TOTALS runs through a single FORMAT JSONCompact request (data + meta +
    # totals), appending the totals as the trailing row the reader expects -- the
    # Native query() path is untouched, so there is no second scan.
    from snuba.clickhouse.native import ClickhouseReader

    class FakeFormattedQuery:
        def get_sql(self) -> str:
            return "SELECT project_id, count() FROM t GROUP BY project_id WITH TOTALS"

    client = mock.Mock()
    # JSONCompact returns each row (and the totals row) as a positional array.
    client.raw_query.return_value = json.dumps(
        {
            "meta": [
                {"name": "project_id", "type": "UInt64"},
                {"name": "count()", "type": "UInt64"},
            ],
            "data": [[1, 10], [2, 20]],
            "totals": [0, 30],
        }
    ).encode()

    pool = _make_pool(client)
    reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)

    result = reader.execute(cast(FormattedQuery, FakeFormattedQuery()), with_totals=True)

    # The trailing totals row is split out as totals; only the real rows remain.
    assert result["data"] == [
        {"project_id": 1, "count()": 10},
        {"project_id": 2, "count()": 20},
    ]
    assert result["totals"] == {"project_id": 0, "count()": 30}
    # Single request via JSONCompact; the Native query() path is never used.
    client.query.assert_not_called()
    _, kwargs = client.raw_query.call_args
    assert kwargs["fmt"] == "JSONCompact"
    assert kwargs["settings"]["output_format_json_quote_64bit_integers"] == 0


def test_totals_jsoncompact_uses_original_query_id_and_inherits_settings() -> None:
    # The single JSONCompact request uses the query's own id and inherits its settings.
    client = mock.Mock()
    client.raw_query.return_value = json.dumps(
        {
            "meta": [{"name": "g", "type": "UInt64"}, {"name": "s", "type": "UInt64"}],
            "data": [[1, 10]],
            "totals": [0, 10],
        }
    ).encode()

    pool = _make_pool(client)
    pool.execute_with_totals(
        "SELECT g, sum(v) AS s FROM t GROUP BY g WITH TOTALS",
        query_id="abc-123",
        settings={"max_execution_time": 30},
    )

    _, kwargs = client.raw_query.call_args
    assert kwargs["fmt"] == "JSONCompact"
    assert kwargs["settings"]["query_id"] == "abc-123"
    assert kwargs["settings"]["max_execution_time"] == 30


def test_totals_jsoncompact_decodes_value_types() -> None:
    # JSONCompact values decode to the types the native driver yields. Covers DateTime
    # (string -> datetime so the reader can ISO-format it), Array, and Nullable.
    from snuba.clickhouse.native import ClickhouseReader

    class FakeFormattedQuery:
        def get_sql(self) -> str:
            return "SELECT g, ts, cnt, arr, opt FROM t GROUP BY g WITH TOTALS"

    client = mock.Mock()
    client.raw_query.return_value = json.dumps(
        {
            "meta": [
                {"name": "g", "type": "UInt64"},
                {"name": "ts", "type": "DateTime"},
                {"name": "cnt", "type": "UInt64"},
                {"name": "arr", "type": "Array(UInt64)"},
                {"name": "opt", "type": "Nullable(UInt64)"},
            ],
            "data": [[1, "2023-01-02 03:04:05", 10, [1, 2], None]],
            "totals": [0, "1970-01-01 00:00:00", 10, [1, 2], 5],
        }
    ).encode()

    pool = _make_pool(client)
    reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)
    result = reader.execute(cast(FormattedQuery, FakeFormattedQuery()), with_totals=True)

    row = result["data"][0]
    # DateTime string -> datetime -> ISO string via the reader's transform.
    assert row["ts"] == "2023-01-02T03:04:05+00:00"
    assert row["arr"] == [1, 2]
    assert row["opt"] is None
    totals = result["totals"]
    assert totals["ts"] == "1970-01-01T00:00:00+00:00"
    assert totals["cnt"] == 10
    assert totals["opt"] == 5


def test_coerce_temporal_only_touches_date_and_datetime() -> None:
    # Date/DateTime strings become objects (else the reader's transforms crash on a
    # str); Nullable is unwrapped; every other type passes through untouched.
    from snuba.clickhouse.connect import _coerce_temporal

    assert _coerce_temporal("2023-01-02 03:04:05", "DateTime") == datetime(2023, 1, 2, 3, 4, 5)
    assert _coerce_temporal("2023-01-02 03:04:05", "DateTime('UTC')") == datetime(
        2023, 1, 2, 3, 4, 5
    )
    assert _coerce_temporal("2023-01-02", "Date") == date(2023, 1, 2)
    # Parametrized variants reduce to the base type (DateTime64(9), Date32).
    assert _coerce_temporal("2023-01-02 03:04:05.123456789", "DateTime64(9)") == datetime(
        2023, 1, 2, 3, 4, 5, 123456
    )
    assert _coerce_temporal("2023-01-02", "Date32") == date(2023, 1, 2)
    assert _coerce_temporal("2023-01-02 03:04:05", "Nullable(DateTime)") == datetime(
        2023, 1, 2, 3, 4, 5
    )
    # Non-temporal values and non-strings (incl. None) pass through untouched.
    assert _coerce_temporal(5, "UInt64") == 5
    assert _coerce_temporal(1.5, "Float64") == 1.5
    assert _coerce_temporal(None, "Nullable(DateTime)") is None
    assert _coerce_temporal(0, "DateTime") == 0


def test_empty_non_totals_result_does_not_refetch() -> None:
    # An empty result yields empty meta (zero-byte Native body) and no second query;
    # the meta is synthesized upstream in db_query, not refetched here.
    client = mock.Mock()
    client.query.return_value = FakeQueryResult(result_set=[], column_names=(), column_types=())

    pool = _make_pool(client)
    result = pool.execute(
        "SELECT flags_key, count() AS count FROM t GROUP BY flags_key",
        with_column_types=True,
    )

    assert result.results == []
    assert result.meta == []
    client.raw_query.assert_not_called()


def test_empty_with_totals_returns_meta_and_totals_via_jsoncompact() -> None:
    # A zero-row WITH TOTALS loses its header and totals row over Native/HTTP, but
    # the JSONCompact request still carries both.
    client = mock.Mock()
    client.raw_query.return_value = json.dumps(
        {
            "meta": [{"name": "g", "type": "UInt64"}, {"name": "s", "type": "UInt64"}],
            "data": [],
            "totals": [0, 0],
        }
    ).encode()

    pool = _make_pool(client)
    # The trailing totals row is appended to results; the reader pops it off.
    result = pool.execute_with_totals(
        "SELECT g, sum(v) AS s FROM t WHERE g = 999 GROUP BY g WITH TOTALS",
    )

    assert result.meta == [("g", "UInt64"), ("s", "UInt64")]
    assert result.results == [(0, 0)]
    client.query.assert_not_called()


def test_non_empty_non_totals_query_does_not_refetch() -> None:
    # The common read path (rows present, no WITH TOTALS) must not pay for a
    # second JSON scan -- only WITH TOTALS queries do.
    client = mock.Mock()
    client.query.return_value = FakeQueryResult(
        result_set=[[1, 2]],
        column_names=("a", "b"),
        column_types=(FakeColumnType("UInt8"), FakeColumnType("UInt8")),
    )

    pool = _make_pool(client)
    result = pool.execute("SELECT a, b FROM t", with_column_types=True)

    assert result.meta == [("a", "UInt8"), ("b", "UInt8")]
    client.raw_query.assert_not_called()


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


def test_execute_explain_uses_command_and_returns_text_rows() -> None:
    # execute_explain serves EXPLAIN via command() -- which sends the statement
    # verbatim, no "FORMAT Native" appended -- not the Native query() path.
    # query() would append "FORMAT Native", the inner query swallows it, and the
    # EXPLAIN output returns as text that the Native reader misparses (the
    # snuba-admin "Unrecognized ClickHouse type base: essionList ..." failure, a
    # fragment of the EXPLAIN AST dump read as a column type). command() returns
    # the decoded text, exposed as one single-column row per line.
    client = mock.Mock()
    # command() strips the trailing newline and (no tabs) returns the whole dump
    # as a single string.
    client.command.return_value = (
        "SelectWithUnionQuery (children 1)\n"
        " ExpressionList (children 1)\n"
        "  Identifier query\n"
        " TablesInSelectQuery (children 1)"
    )

    pool = _make_pool(client)
    result = pool.execute_explain("EXPLAIN AST SELECT query FROM system.clusters")

    # Used the text command() path, never the Native query() path.
    client.command.assert_called_once()
    client.query.assert_not_called()

    # One single-column row per explain line, indentation preserved -- the shape
    # the native driver returns for the same EXPLAIN.
    assert result.results == [
        ("SelectWithUnionQuery (children 1)",),
        (" ExpressionList (children 1)",),
        ("  Identifier query",),
        (" TablesInSelectQuery (children 1)",),
    ]
    assert result.meta == [("explain", "String")]


def test_execute_does_not_route_explain_to_command() -> None:
    # The EXPLAIN handling lives behind the dedicated execute_explain() entry
    # point; the generic execute() does not sniff query text. Even an EXPLAIN
    # passed to execute() goes through the Native query() path -- callers that
    # need EXPLAIN over HTTP must use execute_explain instead.
    client = mock.Mock()
    client.query.return_value = FakeQueryResult(
        result_set=[[1]],
        column_names=("x",),
        column_types=(FakeColumnType("UInt8"),),
    )

    pool = _make_pool(client)
    pool.execute("EXPLAIN AST SELECT 1", with_column_types=True)

    client.query.assert_called_once()
    client.command.assert_not_called()


def test_execute_explain_empty_output_returns_no_rows() -> None:
    # An empty response body makes command() return a QuerySummary (here stubbed
    # by a bare Mock, which is neither str/int/list): that must yield zero rows,
    # not a single empty-string row.
    client = mock.Mock()
    client.command.return_value = mock.Mock()  # stand-in for QuerySummary

    pool = _make_pool(client)
    result = pool.execute_explain("EXPLAIN AST SELECT 1")

    assert result.results == []
    assert result.meta == [("explain", "String")]


def test_execute_explain_error_wrapped_and_preserves_code() -> None:
    # Errors from the command() path must be wrapped in a snuba ClickhouseError
    # with the server code preserved -- the system-query validator relies on the
    # code (e.g. UNKNOWN_TABLE) to turn a failed EXPLAIN into a clean rejection.
    from clickhouse_connect.driver.exceptions import DatabaseError

    UNKNOWN_TABLE = 60
    client = mock.Mock()
    client.command.side_effect = DatabaseError("no such table", code=UNKNOWN_TABLE)

    pool = _make_pool(client)
    try:
        pool.execute_explain("EXPLAIN AST SELECT * FROM nope")
        raise AssertionError("expected a ClickhouseError to be raised")
    except ClickhouseError as error:
        assert error.code == UNKNOWN_TABLE


def test_execute_explain_wraps_client_init_errors() -> None:
    # _get_client() opens the connection on first use and can raise on an
    # unreachable host. Like the normal execute() path, execute_explain must run
    # it inside the error-translation context, so the failure surfaces as a snuba
    # ClickhouseError rather than a raw clickhouse-connect error -- the
    # system-query validator's error handling relies on that contract.
    from clickhouse_connect.driver.exceptions import OperationalError

    pool = _make_pool(mock.Mock())
    pool._get_client = mock.Mock(  # type: ignore[method-assign]
        side_effect=OperationalError("connection refused")
    )

    with pytest.raises(ClickhouseError):
        pool.execute_explain("EXPLAIN AST SELECT 1")


def test_native_pool_execute_explain_delegates_to_execute() -> None:
    # The ClickhousePool default (used by the native driver) runs EXPLAIN through
    # the normal execute() path -- the native protocol decodes it fine, so only
    # the connect pool overrides execute_explain.
    from snuba.clickhouse.native import ClickhouseNativePool, ClickhouseResult

    pool = ClickhouseNativePool("host", 9000, "user", "pw", "db")
    sentinel = ClickhouseResult(results=[("ExpressionList (children 1)",)])
    with mock.patch.object(pool, "execute", return_value=sentinel) as execute:
        out = pool.execute_explain("EXPLAIN AST SELECT 1")

    execute.assert_called_once_with("EXPLAIN AST SELECT 1", with_column_types=True)
    assert out is sentinel


def test_native_pool_execute_with_totals_delegates_to_execute() -> None:
    # The native default runs WITH TOTALS through execute() -- the protocol already
    # streams the totals as the trailing row. Only the connect pool overrides this.
    from snuba.clickhouse.native import ClickhouseNativePool, ClickhouseResult

    pool = ClickhouseNativePool("host", 9000, "user", "pw", "db")
    sentinel = ClickhouseResult(results=[(1, 10), (0, 10)])
    with mock.patch.object(pool, "execute", return_value=sentinel) as execute:
        out = pool.execute_with_totals(
            "SELECT g, sum(v) FROM t GROUP BY g WITH TOTALS",
            query_id="qid",
            settings={"max_threads": 4},
        )

    execute.assert_called_once_with(
        "SELECT g, sum(v) FROM t GROUP BY g WITH TOTALS",
        params=None,
        with_column_types=True,
        query_id="qid",
        settings={"max_threads": 4},
        capture_trace=False,
    )
    assert out is sentinel


def test_native_pool_execute_with_totals_robust_uses_execute_robust() -> None:
    # robust=True must route through execute_robust (the retrying path), matching
    # how the reader forwards robust for WITH TOTALS queries.
    from snuba.clickhouse.native import ClickhouseNativePool, ClickhouseResult

    pool = ClickhouseNativePool("host", 9000, "user", "pw", "db")
    sentinel = ClickhouseResult(results=[(1, 10), (0, 10)])
    with mock.patch.object(pool, "execute_robust", return_value=sentinel) as execute_robust:
        out = pool.execute_with_totals(
            "SELECT g, sum(v) FROM t GROUP BY g WITH TOTALS", robust=True
        )

    execute_robust.assert_called_once()
    assert out is sentinel


def test_reader_routes_with_totals_through_execute_with_totals() -> None:
    # The reader routes WITH TOTALS through execute_with_totals (not plain execute),
    # forwarding robust, so each driver handles totals its own way.
    from snuba.clickhouse.native import ClickhouseReader, ClickhouseResult

    class FakeFormattedQuery:
        def get_sql(self) -> str:
            return "SELECT g, sum(v) AS s FROM t GROUP BY g WITH TOTALS"

    pool = mock.Mock()
    pool.execute_with_totals.return_value = ClickhouseResult(
        results=[(1, 10), (0, 10)],
        meta=[("g", "UInt64"), ("s", "UInt64")],
    )

    reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)
    result = reader.execute(
        cast(FormattedQuery, FakeFormattedQuery()), with_totals=True, robust=True
    )

    pool.execute_with_totals.assert_called_once()
    _, kwargs = pool.execute_with_totals.call_args
    assert kwargs["robust"] is True
    # The non-totals path (plain execute / execute_robust) is not taken.
    pool.execute.assert_not_called()
    pool.execute_robust.assert_not_called()
    # The trailing totals row is split out; only the real rows remain as data.
    assert result["data"] == [{"g": 1, "s": 10}]
    assert result["totals"] == {"g": 0, "s": 10}


@pytest.mark.clickhouse_db
def test_connect_driver_matches_native_for_totals_and_empty_results() -> None:
    # End-to-end vs a real ClickHouse: the connect (HTTP) pool must produce the same
    # reader output as the native pool for the two shapes that broke when the HTTP
    # driver was enabled -- WITH TOTALS and zero-row queries. Mocked tests can't catch
    # these; they hinge on the real server's Native/HTTP serialization.
    from snuba import settings
    from snuba.clickhouse.native import ClickhouseNativePool, ClickhouseReader

    conf = settings.CLUSTERS[0]
    native_pool = ClickhouseNativePool(
        conf["host"], conf["port"], conf["user"], conf["password"], conf["database"]
    )
    connect_pool = ClickhouseConnectPool(
        conf["host"],
        conf["user"],
        conf["password"],
        conf["database"],
        http_port=conf["http_port"],
    )

    # Unique table name so parallel (xdist) workers don't collide.
    table = f"test_connect_totals_parity_{uuid.uuid4().hex[:8]}"

    class FakeFormattedQuery:
        def __init__(self, sql: str) -> None:
            self._sql = sql

        def get_sql(self) -> str:
            return self._sql

    def run(pool: Any, sql: str, with_totals: bool) -> Any:
        reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)
        return reader.execute(
            cast(FormattedQuery, FakeFormattedQuery(sql)), with_totals=with_totals
        )

    try:
        native_pool.execute(f"DROP TABLE IF EXISTS {table}")
        native_pool.execute(
            f"CREATE TABLE {table} (g UInt64, ts DateTime, v UInt64) ENGINE = Memory"
        )
        native_pool.execute(
            f"INSERT INTO {table} (g, ts, v) VALUES",
            [
                [1, datetime(2023, 1, 2, 3, 4, 5), 10],
                [2, datetime(2023, 1, 3, 0, 0, 0), 30],
            ],
        )

        # 1) WITH TOTALS over a DateTime group: data and totals (including the
        #    coerced totals datetime) must match the native driver exactly.
        totals_sql = f"SELECT g, ts, sum(v) AS s FROM {table} GROUP BY g, ts WITH TOTALS ORDER BY g"
        native = run(native_pool, totals_sql, True)
        http = run(connect_pool, totals_sql, True)
        assert http["data"] == native["data"]
        assert http["totals"] == native["totals"]
        assert http["totals"]["s"] == 40  # sum(v) over all rows
        assert {c["name"] for c in http["meta"]} == {"g", "ts", "s"}

        # 2) Zero-row query: native still reports its columns, connect returns empty
        #    meta (zero-byte Native body); meta is synthesized in db_query, not
        #    refetched. Pins the driver-level divergence.
        empty_sql = f"SELECT g, sum(v) AS s FROM {table} WHERE g = 999 GROUP BY g"
        native_empty = run(native_pool, empty_sql, False)
        http_empty = run(connect_pool, empty_sql, False)
        assert http_empty["data"] == []
        assert http_empty["meta"] == []
        assert {c["name"] for c in native_empty["meta"]} == {"g", "s"}

        # 3) Zero-row WITH TOTALS still yields a totals row on both drivers -- the
        #    case that fired the reader's assertion -> "SnubaError" over HTTP.
        #    Asserting the native side pins that its protocol keeps the empty totals row.
        empty_totals_sql = (
            f"SELECT g, sum(v) AS s FROM {table} WHERE g = 999 GROUP BY g WITH TOTALS"
        )
        native_empty_totals = run(native_pool, empty_totals_sql, True)
        http_empty_totals = run(connect_pool, empty_totals_sql, True)
        assert native_empty_totals["data"] == []
        assert native_empty_totals["totals"]["s"] == 0
        assert http_empty_totals["data"] == []
        assert http_empty_totals["totals"] == native_empty_totals["totals"]
        assert {c["name"] for c in http_empty_totals["meta"]} == {"g", "s"}
    finally:
        try:
            native_pool.execute(f"DROP TABLE IF EXISTS {table}")
        finally:
            native_pool.close()
            connect_pool.close()
