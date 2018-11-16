import logging
import re
import time

from six.moves import queue, range

from clickhouse_driver import Client, errors

from snuba import settings


logger = logging.getLogger('snuba.clickhouse')


ESCAPE_RE = re.compile(r'^-?[a-zA-Z][a-zA-Z0-9_\.]*$')
NEGATE_RE = re.compile(r'^(-?)(.*)$')


def escape_col(col):
    if not col:
        return col
    elif ESCAPE_RE.match(col):
        return col
    else:
        return u'{}`{}`'.format(*NEGATE_RE.match(col).groups())


class ClickhousePool(object):
    def __init__(self,
                 host=settings.CLICKHOUSE_SERVER.split(':')[0],
                 port=int(settings.CLICKHOUSE_SERVER.split(':')[1]),
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

    def execute_robust(self, *args, **kwargs):
        retries = 3
        while True:
            try:
                return self.execute(*args, **kwargs)
            except (errors.NetworkError, errors.SocketTimeoutError) as e:
                logger.warning("Write to ClickHouse failed: %s (%d retries)", str(e), retries)
                if retries <= 0:
                    raise
                retries -= 1

                if self.metrics:
                    self.metrics.increment('clickhouse.network-error')

                time.sleep(1)
                continue
            except errors.ServerException as e:
                logger.warning("Write to ClickHouse failed: %s (retrying)", str(e))
                if e.code == errors.ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES:
                    if self.metrics:
                        self.metrics.increment('clickhouse.too-many-queries')

                    time.sleep(1)
                    continue
                else:
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


class ColumnType(object):
    def __str__(self):
        return self.__class__.__name__

    def __repr__(self):
        return str(self)

    def flatten(self, prefix):
        return [(prefix, self)]


class Nullable(ColumnType):
    def __init__(self, inner_type):
        self.inner_type = inner_type

    def __str__(self):
        return u'Nullable({})'.format(str(self.inner_type))


class Array(ColumnType):
    def __init__(self, inner_type):
        self.inner_type = inner_type

    def __str__(self):
        return u'Array({})'.format(str(self.inner_type))


class Nested(ColumnType):
    def __init__(self, nested_columns):
        self.nested_columns = nested_columns

    def __str__(self):
        return u'Nested({})'.format(u", ".join(
            u"{} {}".format(str(column_name), str(column_type))
            for column_name, column_type in self.nested_columns
        ))

    def flatten(self, prefix):
        return [
            ("{}.{}".format(prefix, column_name), Array(column_type))
            for column_name, column_type in self.nested_columns
        ]


class String(ColumnType):
    pass


class FixedString(ColumnType):
    def __init__(self, length):
        self.length = length

    def __str__(self):
        return 'FixedString({})'.format(self.length)


class UInt8(ColumnType):
    pass


class UInt16(ColumnType):
    pass


class UInt32(ColumnType):
    pass


class UInt64(ColumnType):
    pass


class Float32(ColumnType):
    pass


class DateTime(ColumnType):
    pass


class ColumnSet(object):
    """\
    A set of columns, unique by column name.

    Initialized with a list of (column_name: String, column_type: ColumnType) tuples.

    Offers simple functionality:
    * ColumnSets can be added together (order is maintained)
    * Columns can be looked up by ClickHouse normalized names, e.g. 'tags.key'
    * `column_names` and `escaped_column_names` offer (same ordered) lists of
      the normalized column names.
    """
    def __init__(self, columns):
        self.columns = columns

        self.column_names = []
        self.escaped_column_names = []
        self.lookup = {}
        self.flattened = self.flattened()

        for column_name, column_type in self.flattened:
            if column_name in self.lookup:
                raise RuntimeError("Duplicate column: {}".format(column_name))

            self.lookup[column_name] = column_type
            self.column_names.append(column_name)
            self.escaped_column_names.append(escape_col(column_name))

            # also store it by the escaped name
            self.lookup[escape_col(column_name)] = column_type

    def __str__(self):
        return ', '.join(
            '{} {}'.format(escape_col(column_name), column_type)
            for column_name, column_type in self.columns
        )

    def __repr__(self):
        return str(self)

    def __len__(self):
        return len(self.flattened)

    def __add__(self, other):
        if isinstance(other, ColumnSet):
            return ColumnSet(self.columns + other.columns)
        return ColumnSet(self.columns + other)

    def __contains__(self, key):
        return key in self.lookup

    def __getitem__(self, key):
        return self.lookup[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def flattened(self):
        flattened = []
        for column_name, column_type in self.columns:
            flattened.extend(column_type.flatten(column_name))
        return flattened


METADATA_COLUMNS = ColumnSet([
    # optional stream related data
    ('offset', Nullable(UInt64())),
    ('partition', Nullable(UInt16())),
])

PROMOTED_TAG_COLUMNS = ColumnSet([
    # These are the classic tags, they are saved in Snuba exactly as they
    # appear in the event body.
    ('level', Nullable(String())),
    ('logger', Nullable(String())),
    ('server_name', Nullable(String())),  # future name: device_id?
    ('transaction', Nullable(String())),
    ('environment', Nullable(String())),
    ('sentry:release', Nullable(String())),
    ('sentry:dist', Nullable(String())),
    ('sentry:user', Nullable(String())),
    ('site', Nullable(String())),
    ('url', Nullable(String())),
])

PROMOTED_CONTEXT_TAG_COLUMNS = ColumnSet([
    # These are promoted tags that come in in `tags`, but are more closely
    # related to contexts.  To avoid naming confusion with Clickhouse nested
    # columns, they are stored in the database with s/./_/
    # promoted tags
    ('app_device', Nullable(String())),
    ('device', Nullable(String())),
    ('device_family', Nullable(String())),
    ('runtime', Nullable(String())),
    ('runtime_name', Nullable(String())),
    ('browser', Nullable(String())),
    ('browser_name', Nullable(String())),
    ('os', Nullable(String())),
    ('os_name', Nullable(String())),
    ('os_rooted', Nullable(UInt8())),
])

PROMOTED_CONTEXT_COLUMNS = ColumnSet([
    ('os_build', Nullable(String())),
    ('os_kernel_version', Nullable(String())),
    ('device_name', Nullable(String())),
    ('device_brand', Nullable(String())),
    ('device_locale', Nullable(String())),
    ('device_uuid', Nullable(String())),
    ('device_model_id', Nullable(String())),
    ('device_arch', Nullable(String())),
    ('device_battery_level', Nullable(Float32())),
    ('device_orientation', Nullable(String())),
    ('device_simulator', Nullable(UInt8())),
    ('device_online', Nullable(UInt8())),
    ('device_charging', Nullable(UInt8())),
])

REQUIRED_COLUMNS = ColumnSet([
    ('event_id', FixedString(32)),
    ('project_id', UInt64()),
    ('group_id', UInt64()),
    ('timestamp', DateTime()),
    ('deleted', UInt8()),
    ('retention_days', UInt16()),
])

ALL_COLUMNS = REQUIRED_COLUMNS + [
    # required for non-deleted
    ('platform', Nullable(String())),
    ('message', Nullable(String())),
    ('primary_hash', Nullable(FixedString(32))),
    ('received', Nullable(DateTime())),

    # optional user
    ('user_id', Nullable(String())),
    ('username', Nullable(String())),
    ('email', Nullable(String())),
    ('ip_address', Nullable(String())),

    # optional geo
    ('geo_country_code', Nullable(String())),
    ('geo_region', Nullable(String())),
    ('geo_city', Nullable(String())),

    # optional misc
    ('sdk_name', Nullable(String())),
    ('sdk_version', Nullable(String())),
    ('type', Nullable(String())),
    ('version', Nullable(String())),
] + METADATA_COLUMNS \
  + PROMOTED_CONTEXT_COLUMNS \
  + PROMOTED_TAG_COLUMNS \
  + PROMOTED_CONTEXT_TAG_COLUMNS \
  + [
    # other tags
    ('tags', Nested([
        ('key', String()),
        ('value', String()),
    ])),

    # other context
    ('contexts', Nested([
        ('key', String()),
        ('value', String()),
    ])),

    # interfaces

    # http interface
    ('http_method', Nullable(String())),
    ('http_referer', Nullable(String())),

    # exception interface
    ('exception_stacks', Nested([
        ('type', Nullable(String())),
        ('value', Nullable(String())),
        ('mechanism_type', Nullable(String())),
        ('mechanism_handled', Nullable(UInt8())),
    ])),
    ('exception_frames', Nested([
        ('abs_path', Nullable(String())),
        ('filename', Nullable(String())),
        ('package', Nullable(String())),
        ('module', Nullable(String())),
        ('function', Nullable(String())),
        ('in_app', Nullable(UInt8())),
        ('colno', Nullable(UInt32())),
        ('lineno', Nullable(UInt32())),
        ('stack_level', UInt16()),
    ])),
]

# The set of columns, and associated keys that have been promoted
# to the top level table namespace.
PROMOTED_COLS = {
    'tags': frozenset(PROMOTED_TAG_COLUMNS.column_names + PROMOTED_CONTEXT_TAG_COLUMNS.column_names),
    'contexts': frozenset(PROMOTED_CONTEXT_COLUMNS.column_names),
}

# For every applicable promoted column,  a map of translations from the column
# name  we save in the database to the tag we receive in the query.
COLUMN_TAG_MAP = {
    'tags': {t: t.replace('_', '.') for t in PROMOTED_CONTEXT_TAG_COLUMNS.column_names},
    'contexts': {},
}

# And a reverse map from the tags the client expects to the database columns
TAG_COLUMN_MAP = {
    col: dict(map(reversed, trans.items())) for col, trans in COLUMN_TAG_MAP.items()
}

# The canonical list of foo.bar strings that you can send as a `tags[foo.bar]` query
# and they can/will use a promoted column.
PROMOTED_TAGS = {
    col: [COLUMN_TAG_MAP[col].get(x, x) for x in PROMOTED_COLS[col]]
    for col in PROMOTED_COLS
}


def get_table_definition(name, engine, schema=ALL_COLUMNS):
    return """
    CREATE TABLE IF NOT EXISTS %(name)s (%(columns)s) ENGINE = %(engine)s""" % {
        'columns': schema,
        'engine': engine,
        'name': name,
    }


def get_test_engine(
        order_by=settings.DEFAULT_ORDER_BY,
        partition_by=settings.DEFAULT_PARTITION_BY,
        version_column=settings.DEFAULT_VERSION_COLUMN,
        sample_expr=settings.DEFAULT_SAMPLE_EXPR):
    return """
        ReplacingMergeTree(%(version_column)s)
        PARTITION BY %(partition_by)s
        ORDER BY %(order_by)s
        SAMPLE BY %(sample_expr)s ;""" % {
        'order_by': settings.DEFAULT_ORDER_BY,
        'partition_by': settings.DEFAULT_PARTITION_BY,
        'version_column': settings.DEFAULT_VERSION_COLUMN,
        'sample_expr': settings.DEFAULT_SAMPLE_EXPR,
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
