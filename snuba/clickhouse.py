from clickhouse_driver import Client

from snuba import settings
from snuba import util


class Clickhouse(object):
    def __init__(self,
                 host=settings.CLICKHOUSE_SERVER.split(':')[0],
                 port=int(settings.CLICKHOUSE_SERVER.split(':')[1])):
        self.host = host
        self.port = port

    def __enter__(self):
        self.clickhouse = Client(
            host=self.host,
            port=self.port,
            connect_timeout=1,
        )

        return self.clickhouse

    def __exit__(self, *args):
        self.clickhouse.disconnect()


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
