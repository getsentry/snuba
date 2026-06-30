from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from threading import Lock
from typing import Any

import clickhouse_connect
import sentry_sdk
from clickhouse_connect import common as clickhouse_connect_common
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError, OperationalError
from clickhouse_connect.driver.httputil import get_pool_manager

from snuba import environment, settings, state
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.native import (
    ClickhousePool,
    ClickhouseProfile,
    ClickhouseResult,
    Params,
)
from snuba.reader import unwrap_nullable_type
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger("snuba.clickhouse.connect")

metrics = MetricsWrapper(environment.metrics, "clickhouse.connect")

# Stand-in for "no read timeout" on the HTTP path. The native driver maps a
# profile with no timeout (``None``) to an unbounded socket, but clickhouse-connect
# cannot safely take ``None`` (its progress-interval computation does arithmetic
# on the value and would fail), so we pass a very large finite timeout instead —
# effectively unbounded for any real operation. Per-profile timeouts that are set
# (e.g. 25s for reads, longer for migrations) are honored as-is.
UNBOUNDED_SEND_RECEIVE_TIMEOUT_SECONDS = 86_400  # 24h

# Default ClickHouse HTTP port, used when a caller does not pass one.
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123

# clickhouse-connect raises a ProgrammingError by default when it is asked to
# send a setting it considers unknown or readonly. The native driver simply
# forwards whatever settings it is given to the server, so to preserve parity
# we tell clickhouse-connect to drop unrecognized settings instead of failing.
clickhouse_connect_common.set_setting("invalid_setting_action", "drop")

# Matches the ``WITH TOTALS`` clause the query formatter appends for
# ``has_totals()`` queries (see snuba.clickhouse.formatter.query). clickhouse-
# connect's Native/HTTP path never returns the totals row, so the connect pool
# detects the clause to recover the totals via a secondary JSON query. A false
# positive (e.g. the literal text inside a string) is harmless: the recovery
# only appends a row if the JSON response actually carries a ``totals`` block.
_WITH_TOTALS_RE = re.compile(r"\bWITH\s+TOTALS\b", re.IGNORECASE)


def _coerce_json_value(value: Any, ch_type: str) -> Any:
    """
    Convert a scalar parsed from ClickHouse's JSON output into the Python type
    the native result path would have produced. Only ``Date``/``DateTime``
    columns need this: the JSON format renders them as strings, but the reader's
    column-type transforms (and the native driver) expect ``date``/``datetime``
    objects. Numbers already arrive as numbers (we disable 64-bit-int quoting),
    UUIDs are stringified by the reader regardless, and every other type is
    returned unchanged.

    This is applied to the recovered ``WITH TOTALS`` row so its values are
    indistinguishable from the equivalent native-driver row downstream.
    """
    if not isinstance(value, str):
        return value
    _, inner = unwrap_nullable_type(ch_type)
    # Order matters: DateTime / DateTime64 must be checked before the bare Date
    # prefix so they are not misclassified as a date.
    if inner.startswith("DateTime"):
        return datetime.fromisoformat(value)
    if inner.startswith("Date"):
        return date.fromisoformat(value)
    return value


class ClickhouseConnectPool(ClickhousePool):
    """
    HTTP based ClickHouse client backed by ``clickhouse-connect``.

    It subclasses :class:`snuba.clickhouse.native.ClickhousePool` and overrides
    the ``execute`` / ``execute_robust`` / ``close`` interface so it is a true
    drop-in replacement. The decision of which pool to instantiate is made by
    the connection cache (see :mod:`snuba.clusters.cluster`), one level above
    the individual drivers.

    Unlike the native pool, this class does not maintain its own queue of
    connections: ``clickhouse-connect`` manages an HTTP connection pool (via
    ``urllib3``) for us. A single :class:`Client` is created lazily and reused
    across threads, with the underlying pool sized to ``max_pool_size``.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        http_port: int = DEFAULT_CLICKHOUSE_HTTP_PORT,
        secure: bool = False,
        ca_certs: str | None = None,
        verify: bool | None = False,
        connect_timeout: int = 1,
        send_receive_timeout: int | None = 35,
        client_settings: Mapping[str, Any] = {},
    ) -> None:
        # No native connection queue here; clickhouse-connect manages its own
        # HTTP pool. ``port`` is the abstract base attribute (it holds the
        # cluster's configured HTTP port for this driver). The pool size is not
        # a construction parameter: it is always taken from the
        # ``clickhouse_connect_pool_size`` runtime config (see _get_client), so
        # it can be tuned at runtime without rebuilding pools.
        self.host = host
        self.port = http_port
        self.user = user
        self.password = password
        self.database = database
        self.secure = secure
        self.ca_certs = ca_certs
        self.verify = verify
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.client_settings = client_settings

        self.__client: Client | None = None
        self.__lock = Lock()

    def _get_client(self) -> Client:
        # The client (and its handshake with the server) is created lazily so
        # that simply constructing a pool does not open a connection.
        if self.__client is None:
            with self.__lock:
                if self.__client is None:
                    # Pool size always comes from the clickhouse_connect_pool_size
                    # runtime config, falling back to the configured
                    # CLICKHOUSE_MAX_POOL_SIZE. The value is read once, when the
                    # (cached) client is first created.
                    pool_size = (
                        state.get_int_config(
                            "clickhouse_connect_pool_size", settings.CLICKHOUSE_MAX_POOL_SIZE
                        )
                        or settings.CLICKHOUSE_MAX_POOL_SIZE
                    )
                    pool_mgr = get_pool_manager(
                        ca_cert=self.ca_certs,
                        verify=bool(self.verify),
                        maxsize=pool_size,
                        # All requests go to a single host, so a single pool is
                        # enough. Keep a small margin for safety.
                        num_pools=2,
                    )
                    self.__client = clickhouse_connect.get_client(
                        host=self.host,
                        port=self.port,
                        username=self.user,
                        password=self.password,
                        database=self.database,
                        interface="https" if self.secure else "http",
                        secure=self.secure,
                        verify=bool(self.verify),
                        ca_cert=self.ca_certs,
                        connect_timeout=self.connect_timeout,
                        # Honor the per-profile timeout as-is, like the native
                        # driver does (reads get 25s, migrations/DDL keep their
                        # longer timeouts). A profile with no timeout means
                        # "unbounded" on the native path; emulate that here with a
                        # large finite timeout, since clickhouse-connect cannot
                        # take None.
                        send_receive_timeout=(
                            self.send_receive_timeout
                            if self.send_receive_timeout is not None
                            else UNBOUNDED_SEND_RECEIVE_TIMEOUT_SECONDS
                        ),
                        settings=dict(self.client_settings),
                        pool_mgr=pool_mgr,
                        # The native driver applies no implicit row limit; match
                        # that behavior here.
                        query_limit=0,
                        # Sessions serialize queries on the server. We share a
                        # single client across threads, so sessions must be
                        # disabled to allow concurrent queries.
                        autogenerate_session_id=False,
                    )
        return self.__client

    def _build_query_settings(
        self,
        settings: Mapping[str, Any] | None,
        query_id: str | None,
        capture_trace: bool,
    ) -> Mapping[str, Any] | None:
        query_settings = dict(settings) if settings else {}
        if query_id is not None:
            query_settings["query_id"] = query_id
        if capture_trace:
            # We still ask the server to emit trace logs, but unlike the native
            # driver clickhouse-connect does not surface them (it only reads the
            # X-ClickHouse-Summary header), so ``trace_output`` ends up empty on
            # this path. See the note in _execute_once. Practically this means
            # the snuba-admin trace view and its profile-events parsing return
            # nothing when the HTTP driver is enabled; every other admin query
            # path is driver-agnostic. Reconstructing traces over HTTP would
            # require querying system.text_log by query_id (a separate feature).
            query_settings["send_logs_level"] = "trace"
        return query_settings or None

    def _execute_once(
        self,
        query: str,
        params: Params,
        with_column_types: bool,
        query_id: str | None,
        settings: Mapping[str, Any] | None,
        columnar: bool,
        capture_trace: bool,
    ) -> ClickhouseResult:
        client = self._get_client()
        query_settings = self._build_query_settings(settings, query_id, capture_trace)

        with sentry_sdk.start_span(description=query, op="db.clickhouse") as span:
            span.set_data(sentry_sdk.consts.SPANDATA.DB_SYSTEM, "clickhouse")
            span.set_data("query_id", query_id)
            span.set_data("settings", query_settings)
            query_result = client.query(
                query,
                parameters=params if params else None,
                settings=query_settings,
                column_oriented=columnar,
            )

        summary = query_result.summary or {}

        def _int(key: str) -> int:
            value = summary.get(key)
            try:
                return int(value) if value is not None else 0
            except (TypeError, ValueError):
                return 0

        elapsed_ns = summary.get("elapsed_ns")
        try:
            elapsed = float(elapsed_ns) / 1e9 if elapsed_ns is not None else 0.0
        except (TypeError, ValueError):
            elapsed = 0.0

        profile_data = ClickhouseProfile(
            blocks=0,
            bytes=_int("read_bytes"),
            elapsed=elapsed,
            progress_bytes=_int("read_bytes"),
            rows=_int("read_rows"),
        )

        results: Sequence[Any] = query_result.result_set

        # trace_output is always empty here: clickhouse-connect has no mechanism
        # for capturing the server's send_logs_level output (it only parses the
        # X-ClickHouse-Summary header for the profile above). This is a known,
        # accepted limitation of the HTTP path — see _build_query_settings.
        if not with_column_types:
            return ClickhouseResult(
                results=results,
                profile=profile_data,
                trace_output="",
            )

        meta: list[tuple[str, str]] = [
            (name, column_type.name)
            for name, column_type in zip(
                query_result.column_names, query_result.column_types, strict=True
            )
        ]

        # A ``GROUP BY ... WITH TOTALS`` query never carries the totals row over
        # clickhouse-connect's Native/HTTP path: ClickHouse does not serialize
        # totals into the Native HTTP output (they only travel over the native
        # TCP protocol), so ``client.query()`` returns just the grouped rows. The
        # driver-agnostic ClickhouseReader, like the native driver, expects the
        # totals as the trailing result row. The totals are available from the
        # JSON output format, so for a WITH TOTALS query we re-run it once with
        # FORMAT JSON, type-coerce the totals row, and append it.
        #
        # The other HTTP/native divergence -- an empty result set coming back
        # with no column header at all (ClickHouse emits a zero-byte Native body
        # for zero rows), which leaves ``meta`` empty -- is handled one layer up
        # in snuba.web.db_query, which synthesizes the column metadata from the
        # query itself instead of paying for a second scan here.
        if _WITH_TOTALS_RE.search(query) is not None:
            recovered_meta, totals = self._recover_meta_and_totals(
                client, query, params, settings, query_id
            )
            # An empty WITH TOTALS result has no Native header either, so fall
            # back to the JSON meta we just fetched (it carries the real types).
            if not meta:
                meta = recovered_meta
            if totals is not None:
                # Order the totals values to match ``meta`` so the reader, which
                # indexes rows positionally against ``meta``, lines them up the
                # same way it does for the native driver's trailing totals row.
                totals_row = tuple(
                    _coerce_json_value(totals.get(name), ch_type) for name, ch_type in meta
                )
                results = [*results, totals_row]

        return ClickhouseResult(
            results=results,
            meta=meta,
            profile=profile_data,
            trace_output="",
        )

    def _recover_meta_and_totals(
        self,
        client: Client,
        query: str,
        params: Params,
        settings: Mapping[str, Any] | None,
        query_id: str | None,
    ) -> tuple[list[tuple[str, str]], Mapping[str, Any] | None]:
        """
        Re-run a ``WITH TOTALS`` ``query`` with ``FORMAT JSON`` and return
        ``(meta, totals)``.

        ``meta`` is the list of ``(name, type)`` column tuples (present in the
        JSON response even when the result is empty); it backfills the column
        metadata when the totals query itself matched zero rows. ``totals`` is
        the raw totals mapping (column name -> value), or ``None`` if the
        response carries no totals block. The JSON output format is the only one
        clickhouse-connect can read that exposes the totals the Native/HTTP path
        omits.

        ``settings`` is the primary query's settings, which already carries every
        functional runtime setting (timeouts, max_threads, readonly, ...); the
        recovery query inherits all of them. ``query_id`` is the primary query's
        id, used to derive a distinct-but-correlated id for this recovery query.
        """
        json_settings: dict[str, Any] = dict(settings) if settings else {}
        # UInt64/Int64 are emitted as quoted strings in JSON by default; turn that
        # off so numeric values come back as numbers, matching the native driver.
        json_settings["output_format_json_quote_64bit_integers"] = 0
        # Tag the recovery query with an id derived from the primary one's so the
        # two are correlatable in ClickHouse's query_log without sharing an id
        # (which would make them indistinguishable). send_logs_level is
        # intentionally not propagated: the HTTP path cannot surface trace output
        # regardless (see _build_query_settings), so it would be pure overhead.
        if query_id is not None:
            json_settings["query_id"] = f"{query_id}_totals"
        raw = client.raw_query(
            query,
            parameters=params if params else None,
            settings=json_settings,
            fmt="JSON",
        )
        payload = json.loads(raw)
        meta = [(column["name"], column["type"]) for column in payload.get("meta", [])]
        totals = payload.get("totals")
        return meta, totals

    @contextmanager
    def _translate_clickhouse_errors(self) -> Iterator[None]:
        # Map clickhouse-connect's transport/server errors onto snuba's
        # ClickhouseError (preserving the server error code), mirroring how the
        # native pool wraps the clickhouse_driver error family. Shared by
        # execute() and execute_explain() so both surface failures identically.
        try:
            yield
        except OperationalError as e:
            # Connection/transport level failures. Mirrors the native pool's
            # handling of NetworkError/SocketTimeoutError by emitting the
            # connection_error metric before surfacing the error.
            metrics.increment(
                "connection_error",
                tags={
                    "host": self.host,
                    "port": str(self.port),
                    "user": self.user,
                    "database": self.database,
                },
            )
            raise ClickhouseError(str(e), code=getattr(e, "code", None) or -1) from e
        except ClickHouseError as e:
            # ClickHouseError is the base class for every clickhouse-connect
            # error (DatabaseError, ProgrammingError, DataError, ...). The native
            # pool likewise wraps the whole clickhouse_driver errors.Error family
            # into ClickhouseError, preserving the server error code when present.
            raise ClickhouseError(str(e), code=getattr(e, "code", None) or -1) from e

    def execute(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
        retryable: bool = True,
    ) -> ClickhouseResult:
        """
        Execute a clickhouse query.

        Unlike :class:`snuba.clickhouse.native.ClickhouseNativePool`, this
        method does not implement any retry logic of its own. Retries (stale
        keep-alive sockets, transport errors and HTTP 429/503/504 responses)
        are handled internally by clickhouse-connect. Notably this means the
        native pool's ``TOO_MANY_SIMULTANEOUS_QUERIES`` backoff is *not*
        replicated: clickhouse-connect does not retry that error, so it is
        surfaced directly to the caller.

        The ``retryable`` argument is accepted for interface parity with the
        native pool but has no effect here.
        """
        with self._translate_clickhouse_errors():
            return self._execute_once(
                query,
                params,
                with_column_types,
                query_id,
                settings,
                columnar,
                capture_trace,
            )

    def execute_robust(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
        retryable: bool = True,
    ) -> ClickhouseResult:
        """
        Mirrors :meth:`ClickhousePool.execute_robust`. Since retries are
        delegated to clickhouse-connect, this is equivalent to :meth:`execute`.
        """
        return self.execute(
            query,
            params=params,
            with_column_types=with_column_types,
            query_id=query_id,
            settings=settings,
            types_check=types_check,
            columnar=columnar,
            capture_trace=capture_trace,
            retryable=retryable,
        )

    def execute_explain(self, query: str) -> ClickhouseResult:
        """
        Run an EXPLAIN statement over HTTP and return its single ``explain`` text
        column, one row per line. Overrides :meth:`ClickhousePool.execute_explain`.

        EXPLAIN needs its own path on this driver. ``query()`` appends
        ``FORMAT Native`` and decodes the response with its binary Native reader;
        for an EXPLAIN that trailing format is consumed by the *inner* query being
        explained, so the EXPLAIN's own output comes back as text and the Native
        reader misfires — the cryptic ``Unrecognized ClickHouse type ...`` error
        (a fragment of the explain dump read as a column type). ``command()``
        instead sends the statement verbatim — no FORMAT appended — and returns
        the decoded text, which we split into one single-column row per line, the
        same shape the native driver returns for the same EXPLAIN.

        This serves the single-column explain output of EXPLAIN AST / QUERY TREE /
        SYNTAX / PLAN / PIPELINE (the kinds admin system-query validation issues);
        the multi-column EXPLAIN ESTIMATE is not used on this path.
        """
        with self._translate_clickhouse_errors():
            client = self._get_client()
            with sentry_sdk.start_span(description=query, op="db.clickhouse") as span:
                span.set_data(sentry_sdk.consts.SPANDATA.DB_SYSTEM, "clickhouse")
                output = client.command(query)
            return self._explain_result(output)

    @staticmethod
    def _explain_result(output: object) -> ClickhouseResult:
        # command() returns the decoded body: a str for our single-column,
        # tab-free explain output (it has already stripped the trailing newline).
        # Normalize the other documented return shapes defensively before
        # splitting into one row per line.
        if isinstance(output, str):
            text = output
        elif isinstance(output, int):
            text = str(output)
        elif isinstance(output, (list, tuple)):
            # command() only returns a sequence when the body contained tab
            # characters; explain output is space-indented and tab-free, so this
            # is defensive. Re-join so the per-line split preserves the layout.
            text = "\t".join(str(part) for part in output)
        else:
            # QuerySummary (empty body) or anything unexpected -> no rows.
            text = ""

        results: list[tuple[str, ...]] = [(line,) for line in text.split("\n")] if text else []
        profile = ClickhouseProfile(
            bytes=0, progress_bytes=0, blocks=0, rows=len(results), elapsed=0.0
        )
        return ClickhouseResult(
            results=results, meta=[("explain", "String")], profile=profile, trace_output=""
        )

    def close(self) -> None:
        # Take the same lock _get_client uses so a concurrent lazy init can't
        # race with teardown (one thread closing the client while another is
        # creating or about to use it).
        with self.__lock:
            if self.__client is not None:
                self.__client.close()
                self.__client = None
