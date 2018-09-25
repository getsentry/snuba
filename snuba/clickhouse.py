import logging

from contextlib import contextmanager
from six.moves import queue, range

from clickhouse_driver import Client, errors

from snuba import settings
from snuba import util


logger = logging.getLogger('snuba.clickhouse')


class ClickhousePool(object):
    def __init__(self,
                 host=settings.CLICKHOUSE_SERVER.split(':')[0],
                 port=int(settings.CLICKHOUSE_SERVER.split(':')[1]),
                 connect_timeout=1,
                 send_receive_timeout=300,
                 max_pool_size=settings.CLICKHOUSE_MAX_POOL_SIZE,
                 client_settings={}
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
        try:
            conn = self.pool.get(block=True)

            # Lazily create connection instances
            if conn is None:
                conn = self._create_conn()

            try:
                return conn.execute(*args, **kwargs)
            except (errors.NetworkError, errors.SocketTimeoutError) as e:
                # Force a reconnection next time
                conn = None
                raise e
        finally:
            self.pool.put(conn, block=False)

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


_REPLICA_CONN_CACHE = None


def get_shard_replica_connections(clickhouse):
    global _REPLICA_CONN_CACHE

    if settings.CLICKHOUSE_CLUSTER:
        if _REPLICA_CONN_CACHE:
            return _REPLICA_CONN_CACHE

        # ALTERs need to be run against each shard, they are then
        # replicated within the shard itself by ClickHouse,
        # so we discover available shard replicas by using the
        # system.clusters table
        replicas = clickhouse.execute(
            """
            SELECT shard_num, host_name, port
            FROM system.clusters
            WHERE cluster = %(cluster)s
            ORDER BY shard_num, host_name
            """,
            {'cluster': settings.CLICKHOUSE_CLUSTER}
        )

        # pick the first replica for each shard
        seen_shards = set()
        replicas = [
            seen_shards.add(row[0]) or (row[1], row[2])
            for row in replicas
            if row[0] not in seen_shards
        ]

        conns = [ClickhousePool(host=r[0], port=r[1]) for r in replicas]

        _REPLICA_CONN_CACHE = conns
        return _REPLICA_CONN_CACHE
    else:
        # local development mode, just return the (only) connection
        return [clickhouse]


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
        version_column=settings.DEFAULT_VERSION_COLUMN,
        sample_expr=settings.DEFAULT_SAMPLE_EXPR):
    return """
        ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/%(name)s', '{replica}', %(version_column)s)
        PARTITION BY %(partition_by)s
        ORDER BY %(order_by)s
        SAMPLE BY %(sample_expr)s;""" % {
        'name': name,
        'order_by': order_by,
        'partition_by': partition_by,
        'version_column': version_column,
        'sample_expr': sample_expr,
    }


def get_distributed_engine(cluster, database, local_table,
                           sharding_key=settings.DEFAULT_SHARDING_KEY):
    return """Distributed(%(cluster)s, %(database)s, %(local_table)s, %(sharding_key)s);""" % {
        'cluster': cluster,
        'database': database,
        'local_table': local_table,
        'sharding_key': sharding_key,
    }


def get_local_table_definition():
    return get_table_definition(
        settings.DEFAULT_LOCAL_TABLE, get_replicated_engine(name=settings.DEFAULT_LOCAL_TABLE)
    )


def get_dist_table_definition():
    assert settings.CLICKHOUSE_CLUSTER, "CLICKHOUSE_CLUSTER is not set."

    return get_table_definition(
        settings.DEFAULT_DIST_TABLE,
        get_distributed_engine(
            cluster=settings.CLICKHOUSE_CLUSTER,
            database='default',
            local_table=settings.DEFAULT_LOCAL_TABLE,
        )
    )
