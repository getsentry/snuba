import logging
import queue
import re
import time
from datetime import date, datetime
from typing import Mapping, Optional
from uuid import UUID

from clickhouse_driver import Client, errors
from dateutil.tz import tz

from snuba import settings
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.sql import SqlQuery
from snuba.reader import Reader, Result, build_result_transformer


logger = logging.getLogger("snuba.clickhouse")


class ClickhousePool(object):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        connect_timeout=1,
        send_receive_timeout=300,
        max_pool_size=settings.CLICKHOUSE_MAX_POOL_SIZE,
        client_settings={},
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.client_settings = client_settings

        self.pool = queue.LifoQueue(max_pool_size)

        # Fill the queue up so that doing get() on it will block properly
        for _ in range(max_pool_size):
            self.pool.put(None)

    def execute(self, *args, **kwargs):
        """
        Execute a clickhouse query with a single quick retry in case of
        connection failure.

        This should smooth over any Clickhouse instance restarts, but will also
        return relatively quickly with an error in case of more persistent
        failures.
        """
        try:
            conn = self.pool.get(block=True)

            attempts_remaining = 2
            while attempts_remaining > 0:
                attempts_remaining -= 1
                # Lazily create connection instances
                if conn is None:
                    conn = self._create_conn()

                try:
                    result = conn.execute(*args, **kwargs)
                    return result
                except (errors.NetworkError, errors.SocketTimeoutError, EOFError) as e:
                    # Force a reconnection next time
                    conn = None
                    if attempts_remaining == 0:
                        if isinstance(e, errors.Error):
                            raise ClickhouseError(e.code, e.message) from e
                        else:
                            raise e
                    else:
                        # Short sleep to make sure we give the load
                        # balancer a chance to mark a bad host as down.
                        time.sleep(0.1)
                except errors.Error as e:
                    raise ClickhouseError(e.code, e.message) from e
        finally:
            self.pool.put(conn, block=False)

    def execute_robust(self, *args, **kwargs):
        """
        Execute a clickhouse query with a bit more tenacity. Make more retry
        attempts, (infinite in the case of too many simultaneous queries
        errors) and wait a second between retries.

        This is used by the writer, which needs to either complete its current
        write successfully or else quit altogether. Note that each retry in this
        loop will be doubled by the retry in execute()
        """
        attempts_remaining = 3

        while True:
            try:
                return self.execute(*args, **kwargs)
            except (errors.NetworkError, errors.SocketTimeoutError, EOFError) as e:
                # Try 3 times on connection issues.
                logger.warning(
                    "Write to ClickHouse failed: %s (%d tries left)",
                    str(e),
                    attempts_remaining,
                )
                attempts_remaining -= 1
                if attempts_remaining <= 0:
                    if isinstance(e, errors.Error):
                        raise ClickhouseError(e.code, e.message) from e
                    else:
                        raise e
                time.sleep(1)
                continue
            except errors.ServerException as e:
                logger.warning("Write to ClickHouse failed: %s (retrying)", str(e))
                if e.code == errors.ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES:
                    # Try forever if the server is overloaded.
                    time.sleep(1)
                    continue
                else:
                    # Quit immediately for other types of server errors.
                    raise ClickhouseError(e.code, e.message) from e
            except errors.Error as e:
                raise ClickhouseError(e.code, e.message) from e

    def _create_conn(self):
        return Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            connect_timeout=self.connect_timeout,
            send_receive_timeout=self.send_receive_timeout,
            settings=self.client_settings,
        )

    def close(self) -> None:
        try:
            while True:
                conn = self.pool.get(block=False)
                if conn:
                    conn.disconnect()
        except queue.Empty:
            pass


def transform_date(value: date) -> str:
    """
    Convert a timezone-naive date object into an ISO 8601 formatted date and
    time string respresentation.
    """
    # XXX: Both Python and ClickHouse date objects do not have time zones, so
    # just assume UTC. (Ideally, we'd have just left these as timezone naive to
    # begin with and not done this transformation at all, since the time
    # portion has no benefit or significance here.)
    return datetime(*value.timetuple()[:6]).replace(tzinfo=tz.tzutc()).isoformat()


def transform_datetime(value: datetime) -> str:
    """
    Convert a timezone-naive datetime object into an ISO 8601 formatted date
    and time string representation.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=tz.tzutc())
    else:
        value = value.astimezone(tz.tzutc())
    return value.isoformat()


def transform_uuid(value: UUID) -> str:
    """
    Convert a UUID object into a string representation.
    """
    return str(value)


transform_column_types = build_result_transformer(
    [
        (re.compile(r"^Date(\(.+\))?$"), transform_date),
        (re.compile(r"^DateTime(\(.+\))?$"), transform_datetime),
        (re.compile(r"^UUID$"), transform_uuid),
    ]
)


class NativeDriverReader(Reader[SqlQuery]):
    def __init__(self, client: ClickhousePool) -> None:
        self.__client = client

    def __transform_result(self, result, with_totals: bool) -> Result:
        """
        Transform a native driver response into a response that is
        structurally similar to a ClickHouse-flavored JSON response.
        """
        data, meta = result

        # XXX: Rows are represented as mappings that are keyed by column or
        # alias, which is problematic when the result set contains duplicate
        # names. To ensure that the column headers and row data are consistent
        # duplicated names are discarded at this stage.
        columns = {c[0]: i for i, c in enumerate(meta)}

        data = [
            {column: row[index] for column, index in columns.items()} for row in data
        ]

        meta = [
            {"name": m[0], "type": m[1]} for m in [meta[i] for i in columns.values()]
        ]

        if with_totals:
            assert len(data) > 0
            totals = data.pop(-1)
            result = {"data": data, "meta": meta, "totals": totals}
        else:
            result = {"data": data, "meta": meta}

        transform_column_types(result)

        return result

    def execute(
        self,
        query: SqlQuery,
        # TODO: move Clickhouse specific arguments into clickhouse.query.Query
        settings: Optional[Mapping[str, str]] = None,
        with_totals: bool = False,
    ) -> Result:
        settings = {**settings} if settings is not None else {}

        kwargs = {}
        if "query_id" in settings:
            kwargs["query_id"] = settings.pop("query_id")

        return self.__transform_result(
            self.__client.execute(
                query.format_sql(), with_column_types=True, settings=settings, **kwargs
            ),
            with_totals=with_totals,
        )
