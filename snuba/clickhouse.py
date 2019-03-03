import logging
import re
import time

from six.moves import queue, range

from clickhouse_driver import Client, errors

from snuba import settings


logger = logging.getLogger('snuba.clickhouse')


SAFE_COL_RE = re.compile(r'^-?[a-zA-Z_][a-zA-Z0-9_\.]*$')
NEGATE_RE = re.compile(r'^(-?)(.*)$')
ESCAPE_COL_RE = re.compile(r"([`\\])")


def escape_col(col):
    if not col:
        return col
    elif SAFE_COL_RE.match(col):
        # Column is safe to use without wrapping.
        return col
    else:
        # Column needs special characters escaped, and to be wrapped with
        # backticks. If the column starts with a '-', keep that outside the
        # backticks as it is not part of the column name, but used by the query
        # generator to signify the sort order if we are sorting by this column.
        col = ESCAPE_COL_RE.sub(r"\\\1", col)
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


class TableSchema(object):
    """
    Represents the full set of columns in a clickhouse table, with accessors
    for getting the local and distributed table definitions.
    """

    def __init__(self):
        self.CLICKHOUSE_CLUSTER = None
        self.DATABASE = None
        self.LOCAL_TABLE = None
        self.DIST_TABLE = None
        # TODO should there be a QUERY_TABLE so that the client doesn't have to understand
        # whether to select from the local or dist table?
        self.CAN_DROP = False

        self.SAMPLE_EXPR = None
        self.ORDER_BY = None
        self.PARTITION_BY = None
        self.VERSION_COLUMN = None
        self.SHARDING_KEY = None
        self.RETENTION_DAYS = None

        self.ALL_COLUMNS = ColumnSet([])

    def get_local_engine(self):
        return """
            ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/%(name)s', '{replica}', %(version_column)s)
            PARTITION BY %(partition_by)s
            ORDER BY %(order_by)s
            SAMPLE BY %(sample_expr)s;""" % {
            'name': self.LOCAL_TABLE,
            'order_by': self.ORDER_BY,
            'partition_by': self.PARTITION_BY,
            'version_column': self.VERSION_COLUMN,
            'sample_expr': self.SAMPLE_EXPR,
        }

    def get_distributed_engine(self):
        return """Distributed(%(cluster)s, %(database)s, %(local_table)s, %(sharding_key)s);""" % {
            'cluster': self.CLICKHOUSE_CLUSTER,
            'database': self.DATABASE,
            'local_table': self.LOCAL_TABLE,
            'sharding_key': self.SHARDING_KEY,
        }

    def get_table_definition(self, name, engine):
        return """
        CREATE TABLE IF NOT EXISTS %(name)s (%(columns)s) ENGINE = %(engine)s""" % {
            'columns': self.ALL_COLUMNS.for_schema(),
            'engine': engine,
            'name': name,
        }

    def get_local_table_definition(self):
        return self.get_table_definition(
            self.LOCAL_TABLE,
            self.get_local_engine()
        )

    # TODO what calls this?
    def get_dist_table_definition(self):
        assert self.CLICKHOUSE_CLUSTER, "CLICKHOUSE_CLUSTER is not set."

        return self.get_table_definition(
            self.DIST_TABLE,
            self.get_distributed_engine()
        )

    def get_local_table_drop(self):
        if not self.CAN_DROP:
            raise RuntimeError("Can't drop this")

        return "DROP TABLE IF EXISTS %s" % self.LOCAL_TABLE

    def migrate(self, clickhouse):
        pass


class EventsTableSchema(TableSchema):
    def __init__(self, *args, **kwargs):
        super(EventsTableSchema, self).__init__(*args, **kwargs)

        self.CLICKHOUSE_CLUSTER = None
        self.DATABASE = 'default'
        self.LOCAL_TABLE = 'sentry_local'
        self.DIST_TABLE = 'sentry_dist'
        self.QUERY_TABLE = self.DIST_TABLE # For prod, queries are run against the dist table
        self.CAN_DROP = False

        self.SAMPLE_EXPR = 'cityHash64(toString(event_id))'
        self.ORDER_BY = '(project_id, toStartOfDay(timestamp), %s)' % self.SAMPLE_EXPR
        self.PARTITION_BY = '(toMonday(timestamp), if(equals(retention_days, 30), 30, 90))'
        self.VERSION_COLUMN = 'deleted'
        self.SHARDING_KEY = 'cityHash64(toString(event_id))'
        self.RETENTION_DAYS = 90

        self.METADATA_COLUMNS = ColumnSet([
            # optional stream related data
            ('offset', Nullable(UInt(64))),
            ('partition', Nullable(UInt(16))),
        ])

        self.PROMOTED_TAG_COLUMNS = ColumnSet([
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

        self.PROMOTED_CONTEXT_TAG_COLUMNS = ColumnSet([
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

        self.PROMOTED_CONTEXT_COLUMNS = ColumnSet([
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

        self.REQUIRED_COLUMNS = ColumnSet([
            ('event_id', FixedString(32)),
            ('project_id', UInt(64)),
            ('group_id', UInt(64)),
            ('timestamp', DateTime()),
            ('deleted', UInt(8)),
            ('retention_days', UInt(16)),
        ])

        self.ALL_COLUMNS = self.REQUIRED_COLUMNS + [
            # required for non-deleted
            ('platform', Nullable(String())),
            ('message', Nullable(String())),
            ('primary_hash', Nullable(FixedString(32))),
            ('received', Nullable(DateTime())),

            ('search_message', Nullable(String())),
            ('title', Nullable(String())),
            ('location', Nullable(String())),

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
            ('type', Nullable(String())),
            ('version', Nullable(String())),
        ] + self.METADATA_COLUMNS \
          + self.PROMOTED_CONTEXT_COLUMNS \
          + self.PROMOTED_TAG_COLUMNS \
          + self.PROMOTED_CONTEXT_TAG_COLUMNS \
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

            # These are columns we added later in the life of the (current) production
            # database. They don't necessarily belong here in a logical/readability sense
            # but they are here to match the order of columns in production becase
            # `insert_distributed_sync` is very sensitive to column existence and ordering.
            ('culprit', Nullable(String())),
            ('sdk_integrations', Array(String())),
            ('modules', Nested([
                ('name', String()),
                ('version', String()),
            ])),
        ]

        # TODO the following mappings are only used by the EventsDataSet to
        # expand tags[] expressions so perhaps they should be defined there.

        # The set of columns, and associated keys that have been promoted
        # to the top level table namespace.
        self.PROMOTED_COLS = {
            'tags': frozenset(col.flattened for col in (self.PROMOTED_TAG_COLUMNS + self.PROMOTED_CONTEXT_TAG_COLUMNS)),
            'contexts': frozenset(col.flattened for col in self.PROMOTED_CONTEXT_COLUMNS),
        }

        # For every applicable promoted column,  a map of translations from the column
        # name  we save in the database to the tag we receive in the query.
        self.COLUMN_TAG_MAP = {
            'tags': {col.flattened: col.flattened.replace('_', '.') for col in self.PROMOTED_CONTEXT_TAG_COLUMNS},
            'contexts': {},
        }

        # And a reverse map from the tags the client expects to the database columns
        self.TAG_COLUMN_MAP = {
            col: dict(map(reversed, trans.items())) for col, trans in self.COLUMN_TAG_MAP.items()
        }

        # The canonical list of foo.bar strings that you can send as a `tags[foo.bar]` query
        # and they can/will use a promoted column.
        self.PROMOTED_TAGS = {
            col: [self.COLUMN_TAG_MAP[col].get(x, x) for x in self.PROMOTED_COLS[col]]
            for col in self.PROMOTED_COLS
        }

# Dev and Test tables use the same column schema as the production schema,
# but return a different (non-replicated) table engine for the local table.
class DevEventsTableSchema(EventsTableSchema):
    def __init__(self, *args, **kwargs):
        super(DevEventsTableSchema, self).__init__(*args, **kwargs)

        self.LOCAL_TABLE = 'dev_events'
        self.QUERY_TABLE = self.LOCAL_TABLE # For dev/test, queries are run against the local table
        self.CAN_DROP = True

    def get_local_engine(self):
        return """
            ReplacingMergeTree(%(version_column)s)
            PARTITION BY %(partition_by)s
            ORDER BY %(order_by)s
            SAMPLE BY %(sample_expr)s ;""" % {
            'order_by': self.ORDER_BY,
            'partition_by': self.PARTITION_BY,
            'version_column': self.VERSION_COLUMN,
            'sample_expr': self.SAMPLE_EXPR,
        }

    def migrate(self, clickhouse):
        from snuba import migrate
        migrate.run(clickhouse, self)


class TestEventsTableSchema(DevEventsTableSchema):
    def __init__(self, *args, **kwargs):
        super(TestEventsTableSchema, self).__init__(*args, **kwargs)

        self.LOCAL_TABLE = 'test_events'
        self.QUERY_TABLE = self.LOCAL_TABLE
        self.CAN_DROP = True

    def migrate(self, clickhouse):
        pass
