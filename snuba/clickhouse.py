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


# These functions are temporary and are used to help the migration of the DDL to
# the dataset abstractions. Please do not introduce dependencies on them.
# We need them right now because there is plenty of code that depends on the DDL of the
# default dataset but that does not have access to the dataset object.

def get_metadata_columns():
    from snuba.datasets.factory import get_dataset
    dataset = get_dataset("events")
    return dataset.get_metadata_columns()


def get_promoted_tag_columns():
    from snuba.datasets.factory import get_dataset
    dataset = get_dataset("events")
    return dataset.get_promoted_tag_columns()


def get_promoted_context_tag_columns():
    from snuba.datasets.factory import get_dataset
    dataset = get_dataset("events")
    return dataset.get_promoted_context_tag_columns()


def get_promoted_context_columns():
    from snuba.datasets.factory import get_dataset
    dataset = get_dataset("events")
    return dataset.get_promoted_context_columns()


def get_all_columns():
    from snuba.datasets.factory import get_dataset
    dataset = get_dataset("events")
    return dataset.get_schema().get_columns()


def get_required_columns():
    from snuba.datasets.factory import get_dataset
    dataset = get_dataset("events")
    return dataset.get_required_columns()


def get_promoted_cols():
    # The set of columns, and associated keys that have been promoted
    # to the top level table namespace.

    return {
        'tags': frozenset(col.flattened for col in (get_promoted_tag_columns() + get_promoted_context_tag_columns())),
        'contexts': frozenset(col.flattened for col in get_promoted_context_columns()),
    }


def get_column_tag_map():
    # For every applicable promoted column,  a map of translations from the column
    # name  we save in the database to the tag we receive in the query.
    return {
        'tags': {col.flattened: col.flattened.replace('_', '.') for col in get_promoted_context_tag_columns()},
        'contexts': {},
    }


def get_tag_column_map():
    # And a reverse map from the tags the client expects to the database columns
    return {
        col: dict(map(reversed, trans.items())) for col, trans in get_column_tag_map().items()
    }


def get_promoted_tags():
    # The canonical list of foo.bar strings that you can send as a `tags[foo.bar]` query
    # and they can/will use a promoted column.
    return {
        col: [get_column_tag_map()[col].get(x, x) for x in get_promoted_cols()[col]]
        for col in get_promoted_cols()
    }
