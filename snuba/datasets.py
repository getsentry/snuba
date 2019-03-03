import re, six

from snuba import clickhouse, schemas, state, util, processor
from snuba.clickhouse import escape_col


class DataSet(object):
    """
    A DataSet defines the complete set of data sources, schemas, and
    transformations that are required to:
        - Consume, transform, and insert data payloads from Kafka into Clickhouse.
        - Define how Snuba API queries are transformed into SQL.
    """

    def __init__(self, *args, **kwargs):
        self.SCHEMA = None
        self.PROCESSOR = None
        self.PROD = False

    def condition_expr(self, condition, body):
        """
        If this dataset has a particular way of turning an individual
        (column, operator, literal) condition tuple into SQL, return it here.
        """
        return None

    def column_expr(self, column_name, body):
        """
        If this dataset has a particular way of turning a column name
        into a SQL expression (eg. special column aliases that evaluate
        to something else), return it here.
        """
        return None


class EventsDataSet(DataSet):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self, *args, **kwargs):
        super(EventsDataSet, self).__init__()

        self.SCHEMA = clickhouse.EventsTableSchema()
        self.PROCESSOR = processor.EventsProcessor(self.SCHEMA)
        self.PROD = True

        # TODO is the query side processing logically its own class too?
        # ie should we have a SCHEMA, EVENT_PROCESSOR, and QUERY_PROCESSOR?

        # TODO use the right redis db where applicable?
        # all our usage of redis might be global anyway and maybe
        # each dataset doesn't need its own
        self.REDIS_DB = 1

        # TODO is this, and TIME_GROUPS really a global feature, not limited to events dataset?
        self.TIME_GROUP_COLUMN = 'time'
        self.TIME_GROUPS = util.dynamicdict(
            lambda sec: 'toDateTime(intDiv(toUInt32(timestamp), {0}) * {0})'.format(
                sec
            ),
            {
                3600: 'toStartOfHour(timestamp)',
                60: 'toStartOfMinute(timestamp)',
                86400: 'toDate(timestamp)',
            },
        )

        # A column name like "tags[url]"
        self.NESTED_COL_EXPR_RE = re.compile('^(tags|contexts)\[([a-zA-Z0-9_\.:-]+)\]$')

    def column_expr(self, column_name, body):
        expr = None
        if column_name == self.TIME_GROUP_COLUMN:
            expr = self.TIME_GROUPS[body['granularity']]
        elif self.NESTED_COL_EXPR_RE.match(column_name):
            expr = self.tag_expr(column_name)
        elif column_name in ['tags_key', 'tags_value']:
            expr = self.tags_expr(column_name, body)
        elif column_name == 'issue':
            expr = 'group_id'
        elif column_name == 'message':
            # Because of the rename from message->search_message without backfill,
            # records will have one or the other of these fields.
            # TODO this can be removed once all data has search_message filled in.
            expr = 'coalesce(search_message, message)'
        return expr

    def tag_expr(self, column_name):
        """
        Return an expression for the value of a single named tag.

        For tags/contexts, we expand the expression depending on whether the tag is
        "promoted" to a top level column, or whether we have to look in the tags map.
        """
        col, tag = self.NESTED_COL_EXPR_RE.match(column_name).group(1, 2)

        # For promoted tags, return the column name.
        if col in self.SCHEMA.PROMOTED_COLS:
            actual_tag = self.SCHEMA.TAG_COLUMN_MAP[col].get(tag, tag)
            if actual_tag in self.SCHEMA.PROMOTED_COLS[col]:
                return self.string_col(actual_tag)

        # For the rest, return an expression that looks it up in the nested tags.
        return u'{col}.value[indexOf({col}.key, {tag})]'.format(
            **{'col': col, 'tag': util.escape_literal(tag)}
        )

    def tags_expr(self, column_name, body):
        """
        Return an expression that array-joins on tags to produce an output with one
        row per tag.
        """
        assert column_name in ['tags_key', 'tags_value']
        col, k_or_v = column_name.split('_', 1)
        nested_tags_only = state.get_config('nested_tags_only', 1)

        # Generate parallel lists of keys and values to arrayJoin on
        if nested_tags_only:
            key_list = '{}.key'.format(col)
            val_list = '{}.value'.format(col)
        else:
            promoted = self.SCHEMA.PROMOTED_COLS[col]
            col_map = self.SCHEMA.COLUMN_TAG_MAP[col]
            key_list = u'arrayConcat([{}], {}.key)'.format(
                u', '.join(u'\'{}\''.format(col_map.get(p, p)) for p in promoted), col
            )
            val_list = u'arrayConcat([{}], {}.value)'.format(
                ', '.join(self.string_col(p) for p in promoted), col
            )

        cols_used = util.all_referenced_columns(body) & set(['tags_key', 'tags_value'])
        if len(cols_used) == 2:
            # If we use both tags_key and tags_value in this query, arrayjoin
            # on (key, value) tag tuples.
            expr = (u'arrayJoin(arrayMap((x,y) -> [x,y], {}, {}))').format(
                key_list, val_list
            )

            # put the all_tags expression in the alias cache so we can use the alias
            # to refer to it next time (eg. 'all_tags[1] AS tags_key'). instead of
            # expanding the whole tags expression again.
            expr = util.alias_expr(expr, 'all_tags', body)
            return u'({})[{}]'.format(expr, 1 if k_or_v == 'key' else 2)
        else:
            # If we are only ever going to use one of tags_key or tags_value, don't
            # bother creating the k/v tuples to arrayJoin on, or the all_tags alias
            # to re-use as we won't need it.
            return 'arrayJoin({})'.format(key_list if k_or_v == 'key' else val_list)

    def condition_expr(self, condition, body):
        lhs, op, lit = condition
        if (
            lhs in ('received', 'timestamp')
            and op in ('>', '<', '>=', '<=', '=', '!=')
            and isinstance(lit, str)
        ):
            lit = util.parse_datetime(lit)

        # If the LHS is a simple column name that refers to an array column
        # (and we are not arrayJoining on that column, which would make it
        # scalar again) and the RHS is a scalar value, we assume that the user
        # actually means to check if any (or all) items in the array match the
        # predicate, so we return an `any(x == value for x in array_column)`
        # type expression. We assume that operators looking for a specific value
        # (IN, =, LIKE) are looking for rows where any array value matches, and
        # exclusionary operators (NOT IN, NOT LIKE, !=) are looking for rows
        # where all elements match (eg. all NOT LIKE 'foo').
        if (
            isinstance(lhs, six.string_types)
            and lhs in self.SCHEMA.ALL_COLUMNS
            and type(self.SCHEMA.ALL_COLUMNS[lhs].type) == clickhouse.Array
            and self.SCHEMA.ALL_COLUMNS[lhs].base_name != body.get('arrayjoin')
            and not isinstance(lit, (list, tuple))
        ):
            any_or_all = (
                'arrayExists' if op in schemas.POSITIVE_OPERATORS else 'arrayAll'
            )
            return u'{}(x -> assumeNotNull(x {} {}), {})'.format(
                any_or_all,
                op,
                util.escape_literal(lit),
                util.column_expr(self, lhs, body),
            )

        return None

    def string_col(self, col):
        col_type = self.SCHEMA.ALL_COLUMNS.get(col, None)
        col_type = str(col_type) if col_type else None

        if col_type and 'String' in col_type and 'FixedString' not in col_type:
            return escape_col(col)
        else:
            return 'toString({})'.format(escape_col(col))


# TODO if the only thing that is different about these is the table name
# it might be better to just have the table name as a constructor parameter
# to EventsDataSet
class TestEventsDataSet(EventsDataSet):
    def __init__(self, *args, **kwargs):
        super(TestEventsDataSet, self).__init__(*args, **kwargs)

        self.SCHEMA = clickhouse.TestEventsTableSchema()
        self.PROCESSOR = processor.EventsProcessor(self.SCHEMA)
        self.PROD = False


class DevEventsDataSet(EventsDataSet):
    def __init__(self, *args, **kwargs):
        super(DevEventsDataSet, self).__init__(*args, **kwargs)

        self.SCHEMA = clickhouse.DevEventsTableSchema()
        self.PROCESSOR = processor.EventsProcessor(self.SCHEMA)
        self.PROD = False
