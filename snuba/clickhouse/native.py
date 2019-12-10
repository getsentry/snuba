import logging
import queue
import time
from typing import Iterable, Mapping, Optional

from clickhouse_driver import Client, errors

from snuba import settings
from snuba.clickhouse.columns import Array
from snuba.clickhouse.query import ClickhouseQuery
from snuba.reader import Reader, Result, transform_columns
from snuba.writer import BatchWriter, WriterTableRow


logger = logging.getLogger("snuba.clickhouse")


class ClickhousePool(object):
    def __init__(
        self,
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_PORT,
        connect_timeout=1,
        send_receive_timeout=300,
        max_pool_size=settings.CLICKHOUSE_MAX_POOL_SIZE,
        client_settings={},
    ):
        self.host = host
        self.port = port
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
                        raise e
                    else:
                        # Short sleep to make sure we give the load
                        # balancer a chance to mark a bad host as down.
                        time.sleep(0.1)
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
                    raise

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
                    raise

    def _create_conn(self):
        return Client(
            host=self.host,
            port=self.port,
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


class NativeDriverReader(Reader[ClickhouseQuery]):
    def __init__(self, client) -> None:
        self.__client = client

    def __transform_result(self, result, with_totals: bool) -> Result:
        """
        Transform a native driver response into a response that is
        structurally similar to a ClickHouse-flavored JSON response.
        """
        data, meta = result

        data = [{c[0]: d[i] for i, c in enumerate(meta)} for d in data]
        meta = [{"name": m[0], "type": m[1]} for m in meta]

        if with_totals:
            assert len(data) > 0
            totals = data.pop(-1)
            result = {"data": data, "meta": meta, "totals": totals}
        else:
            result = {"data": data, "meta": meta}

        return transform_columns(result)

    def execute(
        self,
        query: ClickhouseQuery,
        # TODO: move Clickhouse specific arguments into DictClickhouseQuery
        settings: Optional[Mapping[str, str]] = None,
        query_id: Optional[str] = None,
        with_totals: bool = False,
    ) -> Result:
        if settings is None:
            settings = {}

        kwargs = {}
        if query_id is not None:
            kwargs["query_id"] = query_id

        sql = query.format_sql()
        return self.__transform_result(
            self.__client.execute(
                sql, with_column_types=True, settings=settings, **kwargs
            ),
            with_totals=with_totals,
        )


class NativeDriverBatchWriter(BatchWriter):
    def __init__(self, schema, connection):
        self.__schema = schema
        self.__connection = connection

    def __row_to_column_list(self, columns, row):
        values = []
        for col in columns:
            value = row.get(col.flattened, None)
            if value is None and isinstance(col.type, Array):
                value = []
            values.append(value)
        return values

    def write(self, rows: Iterable[WriterTableRow]):
        columns = self.__schema.get_columns()
        self.__connection.execute_robust(
            "INSERT INTO %(table)s (%(colnames)s) VALUES"
            % {
                "colnames": ", ".join(col.escaped for col in columns),
                "table": self.__schema.get_table_name(),
            },
            [self.__row_to_column_list(columns, row) for row in rows],
            types_check=False,
        )
