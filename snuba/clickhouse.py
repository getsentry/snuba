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


class Column(object):
    def __init__(self, name, type):
        self.name = name
        self.type = type

    def __repr__(self):
        return 'Column({}, {})'.format(repr(self.name), repr(self.type))

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
            and self.name == other.name \
            and self.type == other.type

    def for_schema(self):
        return '{} {}'.format(escape_col(self.name), self.type.for_schema())

    @staticmethod
    def to_columns(columns):
        return [
            Column(*col) if not isinstance(col, Column) else col
            for col in columns
        ]


class FlattenedColumn(object):
    def __init__(self, base_name, name, type):
        self.base_name = base_name
        self.name = name
        self.type = type

        self.flattened = '{}.{}'.format(self.base_name, self.name) if self.base_name else self.name
        self.escaped = escape_col(self.flattened)

    def __repr__(self):
        return 'FlattenedColumn({}, {}, {})'.format(
            repr(self.base_name), repr(self.name), repr(self.type)
        )

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
            and self.flattened == other.flattened \
            and self.type == other.type


class ColumnType(object):
    def __repr__(self):
        return self.__class__.__name__ + '()'

    def __eq__(self, other):
        return self.__class__ == other.__class__

    def for_schema(self):
        return self.__class__.__name__

    def flatten(self, name):
        return [FlattenedColumn(None, name, self)]


class Nullable(ColumnType):
    def __init__(self, inner_type):
        self.inner_type = inner_type

    def __repr__(self):
        return u'Nullable({})'.format(repr(self.inner_type))

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
            and self.inner_type == other.inner_type

    def for_schema(self):
        return u'Nullable({})'.format(self.inner_type.for_schema())


class Array(ColumnType):
    def __init__(self, inner_type):
        self.inner_type = inner_type

    def __repr__(self):
        return u'Array({})'.format(repr(self.inner_type))

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
            and self.inner_type == other.inner_type

    def for_schema(self):
        return u'Array({})'.format(self.inner_type.for_schema())


class Nested(ColumnType):
    def __init__(self, nested_columns):
        self.nested_columns = Column.to_columns(nested_columns)

    def __repr__(self):
        return u'Nested({})'.format(repr(self.nested_columns))

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
            and self.nested_columns == other.nested_columns

    def for_schema(self):
        return u'Nested({})'.format(u", ".join(
            column.for_schema() for column in self.nested_columns
        ))

    def flatten(self, name):
        return [
            FlattenedColumn(name, column.name, Array(column.type))
            for column in self.nested_columns
        ]


class String(ColumnType):
    pass


class FixedString(ColumnType):
    def __init__(self, length):
        self.length = length

    def __repr__(self):
        return 'FixedString({})'.format(self.length)

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
            and self.length == other.length

    def for_schema(self):
        return 'FixedString({})'.format(self.length)


class UInt(ColumnType):
    def __init__(self, size):
        assert size in (8, 16, 32, 64)
        self.size = size

    def __repr__(self):
        return 'UInt({})'.format(self.size)

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
            and self.size == other.size

    def for_schema(self):
        return 'UInt{}'.format(self.size)


class Float(ColumnType):
    def __init__(self, size):
        assert size in (32, 64)
        self.size = size

    def __repr__(self):
        return 'Float({})'.format(self.size)

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
            and self.size == other.size

    def for_schema(self):
        return 'Float{}'.format(self.size)


class DateTime(ColumnType):
    pass


class ColumnSet(object):
    """\
    A set of columns, unique by column name.

    Initialized with a list of Column objects or
    (column_name: String, column_type: ColumnType) tuples.

    Offers simple functionality:
    * ColumnSets can be added together (order is maintained)
    * Columns can be looked up by ClickHouse normalized names, e.g. 'tags.key'
    * `for_schema()` can be used to generate valid ClickHouse column names
      and types for a table schema.
    """
    def __init__(self, columns):
        self.columns = Column.to_columns(columns)

        self._lookup = {}
        self._flattened = []
        for column in self.columns:
            self._flattened.extend(column.type.flatten(column.name))

        for col in self._flattened:
            if col.flattened in self._lookup:
                raise RuntimeError("Duplicate column: {}".format(col.flattened))

            self._lookup[col.flattened] = col
            # also store it by the escaped name
            self._lookup[col.escaped] = col

    def __repr__(self):
        return 'ColumnSet({})'.format(repr(self.columns))

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
            and self._flattened == other._flattened

    def __len__(self):
        return len(self._flattened)

    def __add__(self, other):
        if isinstance(other, ColumnSet):
            return ColumnSet(self.columns + other.columns)
        return ColumnSet(self.columns + other)

    def __contains__(self, key):
        return key in self._lookup

    def __getitem__(self, key):
        return self._lookup[key]

    def __iter__(self):
        return iter(self._flattened)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def for_schema(self):
        return ', '.join(column.for_schema() for column in self.columns)


METADATA_COLUMNS = ColumnSet([
    # optional stream related data
    ('offset', Nullable(UInt(64))),
    ('partition', Nullable(UInt(16))),
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
    ('os_rooted', Nullable(UInt(8))),
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
    ('device_battery_level', Nullable(Float(32))),
    ('device_orientation', Nullable(String())),
    ('device_simulator', Nullable(UInt(8))),
    ('device_online', Nullable(UInt(8))),
    ('device_charging', Nullable(UInt(8))),
])

REQUIRED_COLUMNS = ColumnSet([
    ('event_id', FixedString(32)),
    ('project_id', UInt(64)),
    ('group_id', UInt(64)),
    ('timestamp', DateTime()),
    ('deleted', UInt(8)),
    ('retention_days', UInt(16)),
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

    ('sdk_name', Nullable(String())),
    ('sdk_version', Nullable(String())),
    ('sdk_integrations', Array(String())),
    ('modules', Nested([
        ('name', String()),
        ('version', String()),
    ])),
    ('culprit', Nullable(String())),
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
        ('mechanism_handled', Nullable(UInt(8))),
    ])),
    ('exception_frames', Nested([
        ('abs_path', Nullable(String())),
        ('filename', Nullable(String())),
        ('package', Nullable(String())),
        ('module', Nullable(String())),
        ('function', Nullable(String())),
        ('in_app', Nullable(UInt(8))),
        ('colno', Nullable(UInt(32))),
        ('lineno', Nullable(UInt(32))),
        ('stack_level', UInt(16)),
    ])),
]

# The set of columns, and associated keys that have been promoted
# to the top level table namespace.
PROMOTED_COLS = {
    'tags': frozenset(col.flattened for col in (PROMOTED_TAG_COLUMNS + PROMOTED_CONTEXT_TAG_COLUMNS)),
    'contexts': frozenset(col.flattened for col in PROMOTED_CONTEXT_COLUMNS),
}

# For every applicable promoted column,  a map of translations from the column
# name  we save in the database to the tag we receive in the query.
COLUMN_TAG_MAP = {
    'tags': {col.flattened: col.flattened.replace('_', '.') for col in PROMOTED_CONTEXT_TAG_COLUMNS},
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


def get_table_definition(name, engine, columns=ALL_COLUMNS):
    return """
    CREATE TABLE IF NOT EXISTS %(name)s (%(columns)s) ENGINE = %(engine)s""" % {
        'columns': columns.for_schema(),
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
