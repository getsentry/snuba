from six.moves import queue

from clickhouse_driver import Client

from snuba import settings
from snuba import util


class ClickhousePool(object):
    def __init__(self,
                 host=settings.CLICKHOUSE_SERVER.split(':')[0],
                 port=int(settings.CLICKHOUSE_SERVER.split(':')[1]),
                 connect_timeout=1,
                 send_receive_timeout=300,
                 max_pool_size=10,
                 ):
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout

        self.pool = queue.LifoQueue(max_pool_size)

        # Fill the queue up so that doing get() on it will block properly
        for _ in xrange(max_pool_size):
            self.pool.put(None)

    def execute(self, *args, **kwargs):
        try:
            conn = self.pool.get(block=True)

            # Lazily create connection instances
            if conn is None:
                conn = self._create_conn()

            return conn.execute(*args, **kwargs)
        finally:
            self.pool.put(conn, block=False)

    def _create_conn(self):
        return Client(
            host=self.host,
            port=self.port,
            connect_timeout=self.connect_timeout,
            send_receive_timeout=self.send_receive_timeout,
        )

    def close(self):
        try:
            while True:
                conn = self.pool.get(block=False)
                if conn:
                    conn.disconnect()
        except queue.Empty:
            pass


def get_table_definition(name, engine, columns=settings.SCHEMA_COLUMNS):
    return """
    CREATE TABLE IF NOT EXISTS %(name)s (%(columns)s) ENGINE = %(engine)s""" % {
        'columns': ', '.join('{} {}'.format(util.escape_col(col), type_) for col, type_ in columns),
        'engine': engine,
        'name': name,
    }


def get_test_engine(
        order_by=settings.DEFAULT_ORDER_BY,
        partition_by=settings.DEFAULT_PARTITION_BY,
        version_column=settings.DEFAULT_VERSION_COLUMN):
    return """
        ReplacingMergeTree(%(version_column)s)
        PARTITION BY %(partition_by)s
        ORDER BY %(order_by)s;""" % {
        'order_by': settings.DEFAULT_ORDER_BY,
        'partition_by': settings.DEFAULT_PARTITION_BY,
        'version_column': settings.DEFAULT_VERSION_COLUMN
    }


def get_replicated_engine(
        name,
        order_by=settings.DEFAULT_ORDER_BY,
        partition_by=settings.DEFAULT_PARTITION_BY,
        version_column=settings.DEFAULT_VERSION_COLUMN):
    return """
        ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/%(name)s', '{replica}', %(version_column)s)
        PARTITION BY %(partition_by)s
        ORDER BY %(order_by)s;""" % {
        'name': name,
        'order_by': order_by,
        'partition_by': partition_by,
        'version_column': version_column,
    }


def get_distributed_engine(cluster, database, local_table,
                           sharding_key=settings.DEFAULT_SHARDING_KEY):
    return """Distributed(%(cluster)s, %(database)s, %(local_table)s, %(sharding_key)s);""" % {
        'cluster': cluster,
        'database': database,
        'local_table': local_table,
        'sharding_key': sharding_key,
    }
