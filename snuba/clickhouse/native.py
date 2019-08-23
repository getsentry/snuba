import logging
import queue
import time

from clickhouse_driver import Client, errors

from snuba import settings


logger = logging.getLogger('snuba.clickhouse')


class ClickhousePool(object):
    def __init__(self,
                 host=settings.CLICKHOUSE_HOST,
                 port=settings.CLICKHOUSE_PORT,
                 connect_timeout=1,
                 send_receive_timeout=300,
                 max_pool_size=settings.CLICKHOUSE_MAX_POOL_SIZE,
                 client_settings={},
                 metrics=None,
                 ):
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.client_settings = client_settings
        self.metrics = metrics

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
                logger.warning("Write to ClickHouse failed: %s (%d tries left)", str(e), attempts_remaining)
                attempts_remaining -= 1
                if attempts_remaining <= 0:
                    raise

                if self.metrics:
                    self.metrics.increment('clickhouse.network-error')

                time.sleep(1)
                continue
            except errors.ServerException as e:
                logger.warning("Write to ClickHouse failed: %s (retrying)", str(e))
                if e.code == errors.ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES:
                    # Try forever if the server is overloaded.
                    if self.metrics:
                        self.metrics.increment('clickhouse.too-many-queries')
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
            settings=self.client_settings
        )

    def close(self):
        try:
            while True:
                conn = self.pool.get(block=False)
                if conn:
                    conn.disconnect()
        except queue.Empty:
            pass
