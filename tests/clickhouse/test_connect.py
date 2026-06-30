import json
import uuid
from datetime import datetime
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


def test_with_totals_refetched_via_json_over_http() -> None:
    # clickhouse-connect's Native/HTTP path never returns the WITH TOTALS row:
    # ClickHouse does not serialize totals into the Native HTTP output, so
    # client.query() yields only the grouped rows. The connect pool therefore
    # re-fetches the totals with a FORMAT JSON query and appends them as the
    # trailing result row -- the contract the driver-agnostic ClickhouseReader
    # expects, the same one the native driver satisfies by appending the totals
    # row it receives over the native protocol.
    from snuba.clickhouse.native import ClickhouseReader

    class FakeFormattedQuery:
        def get_sql(self) -> str:
            return "SELECT project_id, count() FROM t GROUP BY project_id WITH TOTALS"

    client = mock.Mock()
    # The Native path returns only the grouped rows -- no totals row.
    client.query.return_value = FakeQueryResult(
        result_set=[[1, 10], [2, 20]],
        column_names=("project_id", "count()"),
        column_types=(FakeColumnType("UInt64"), FakeColumnType("UInt64")),
    )
    # The JSON re-fetch is the only place the totals are available.
    client.raw_query.return_value = json.dumps(
        {
            "meta": [
                {"name": "project_id", "type": "UInt64"},
                {"name": "count()", "type": "UInt64"},
            ],
            "data": [
                {"project_id": 1, "count()": 10},
                {"project_id": 2, "count()": 20},
            ],
            "totals": {"project_id": 0, "count()": 30},
        }
    ).encode()

    pool = _make_pool(client)
    reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)

    result = reader.execute(cast(FormattedQuery, FakeFormattedQuery()), with_totals=True)

    # The re-fetched totals row is split out as totals; only the real rows remain.
    assert result["data"] == [
        {"project_id": 1, "count()": 10},
        {"project_id": 2, "count()": 20},
    ]
    assert result["totals"] == {"project_id": 0, "count()": 30}
    # Totals come from the JSON output format, not the Native query path.
    _, kwargs = client.raw_query.call_args
    assert kwargs["fmt"] == "JSON"
    assert kwargs["settings"]["output_format_json_quote_64bit_integers"] == 0


def test_totals_refetch_uses_correlated_query_id_and_inherits_settings() -> None:
    # The WITH TOTALS recovery query carries an id derived from the primary
    # query's id -- so the two correlate in ClickHouse's query_log -- without
    # reusing it (which would make them indistinguishable). Every other runtime
    # setting is inherited from the primary query.
    client = mock.Mock()
    client.query.return_value = FakeQueryResult(
        result_set=[[1, 10]],
        column_names=("g", "s"),
        column_types=(FakeColumnType("UInt64"), FakeColumnType("UInt64")),
    )
    client.raw_query.return_value = json.dumps(
        {
            "meta": [{"name": "g", "type": "UInt64"}, {"name": "s", "type": "UInt64"}],
            "data": [{"g": 1, "s": 10}],
            "totals": {"g": 0, "s": 10},
        }
    ).encode()

    pool = _make_pool(client)
    pool.execute(
        "SELECT g, sum(v) AS s FROM t GROUP BY g WITH TOTALS",
        with_column_types=True,
        query_id="abc-123",
        settings={"max_execution_time": 30},
    )

    _, kwargs = client.raw_query.call_args
    # Correlated but distinct id, and the primary query's functional settings.
    assert kwargs["settings"]["query_id"] == "abc-123_totals"
    assert kwargs["settings"]["max_execution_time"] == 30


def test_empty_non_totals_result_does_not_refetch() -> None:
    # An empty, non-WITH-TOTALS result comes back from the Native/HTTP path with
    # no column header (ClickHouse emits a zero-byte Native body for zero rows),
    # so meta is empty. The driver does NOT pay for a second query to recover it:
    # the column metadata for an empty result is synthesized one layer up, in
    # snuba.web.db_query, from the query's own columns (see
    # test_db_query.test_empty_result_meta_synthesized_from_query).
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


def test_empty_with_totals_recovers_meta_and_totals_via_json() -> None:
    # A WITH TOTALS query that matches zero rows loses both its column header and
    # its totals row over the Native/HTTP path. The JSON re-fetch (which only
    # fires for WITH TOTALS) recovers the column metadata and the totals row.
    client = mock.Mock()
    client.query.return_value = FakeQueryResult(result_set=[], column_names=(), column_types=())
    client.raw_query.return_value = json.dumps(
        {
            "meta": [{"name": "g", "type": "UInt64"}, {"name": "s", "type": "UInt64"}],
            "data": [],
            "totals": {"g": 0, "s": 0},
        }
    ).encode()

    pool = _make_pool(client)
    # The trailing totals row is appended to results; the reader pops it off.
    result = pool.execute(
        "SELECT g, sum(v) AS s FROM t WHERE g = 999 GROUP BY g WITH TOTALS",
        with_column_types=True,
    )

    assert result.meta == [("g", "UInt64"), ("s", "UInt64")]
    assert result.results == [(0, 0)]


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


def test_recovered_totals_datetime_is_typed_like_native() -> None:
    # The totals row recovered from JSON has Date/DateTime values as strings; the
    # pool coerces them to date/datetime objects so they are indistinguishable
    # from the native driver's values and survive the reader's type transforms
    # (which would otherwise crash calling datetime methods on a str).
    from snuba.clickhouse.native import ClickhouseReader

    class FakeFormattedQuery:
        def get_sql(self) -> str:
            return "SELECT ts, count() FROM t GROUP BY ts WITH TOTALS"

    client = mock.Mock()
    client.query.return_value = FakeQueryResult(
        result_set=[[datetime(2023, 1, 2, 3, 4, 5), 10]],
        column_names=("ts", "count()"),
        column_types=(FakeColumnType("DateTime"), FakeColumnType("UInt64")),
    )
    client.raw_query.return_value = json.dumps(
        {
            "meta": [
                {"name": "ts", "type": "DateTime"},
                {"name": "count()", "type": "UInt64"},
            ],
            "data": [{"ts": "2023-01-02 03:04:05", "count()": 10}],
            "totals": {"ts": "1970-01-01 00:00:00", "count()": 10},
        }
    ).encode()

    pool = _make_pool(client)
    reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)
    result = reader.execute(cast(FormattedQuery, FakeFormattedQuery()), with_totals=True)

    # The DateTime totals value (a JSON string) is coerced to a datetime, then
    # transformed to an ISO string -- exactly like the data row.
    assert result["data"][0]["ts"] == "2023-01-02T03:04:05+00:00"
    assert result["totals"]["ts"] == "1970-01-01T00:00:00+00:00"


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


@pytest.mark.clickhouse_db
def test_connect_driver_matches_native_for_totals_and_empty_results() -> None:
    # End-to-end against a real ClickHouse: the clickhouse-connect (HTTP) pool
    # must produce the same ClickhouseReader output as the native pool for the
    # two query shapes that broke when the HTTP driver was first enabled --
    # GROUP BY ... WITH TOTALS, and queries that match zero rows. Mocked unit
    # tests cannot catch these regressions because they hinge on how a real
    # server serializes the Native/HTTP response (an empty body for zero rows,
    # and no totals block at all).
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

        # 2) A query matching zero rows: the native driver still reports its
        #    columns, while the connect driver returns empty meta at the reader
        #    level (ClickHouse sends a zero-byte Native body for zero rows). The
        #    column metadata for this case is synthesized one layer up, in
        #    db_query, from the query's own columns -- see
        #    test_db_query.test_empty_result_meta_synthesized_from_query -- so no
        #    second scan is paid here. This pins the driver-level divergence.
        empty_sql = f"SELECT g, sum(v) AS s FROM {table} WHERE g = 999 GROUP BY g"
        native_empty = run(native_pool, empty_sql, False)
        http_empty = run(connect_pool, empty_sql, False)
        assert http_empty["data"] == []
        assert http_empty["meta"] == []
        assert {c["name"] for c in native_empty["meta"]} == {"g", "s"}

        # 3) WITH TOTALS matching zero rows still yields a totals row. Over the
        #    HTTP driver this used to leave the reader with no rows, firing the
        #    totals assertion -> "SnubaError (No error message)".
        empty_totals_sql = (
            f"SELECT g, sum(v) AS s FROM {table} WHERE g = 999 GROUP BY g WITH TOTALS"
        )
        http_empty_totals = run(connect_pool, empty_totals_sql, True)
        assert http_empty_totals["data"] == []
        assert http_empty_totals["totals"]["s"] == 0
        assert {c["name"] for c in http_empty_totals["meta"]} == {"g", "s"}
    finally:
        try:
            native_pool.execute(f"DROP TABLE IF EXISTS {table}")
        finally:
            native_pool.close()
            connect_pool.close()
