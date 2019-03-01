import logging
import six
import time
from collections import deque
from datetime import datetime
import simplejson as json

from batching_kafka_consumer import AbstractBatchWorker

from . import settings
from snuba.clickhouse import escape_col
from snuba.processor import _hashify, InvalidMessageType, InvalidMessageVersion
from snuba.redis import redis_client
from snuba.util import escape_string


logger = logging.getLogger('snuba.replacer')


# TODO should this be in clickhouse.py
CLICKHOUSE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

EXCLUDE_GROUPS = object()
NEEDS_FINAL = object()


# TODO the whole needs_final calculation here is also specific to the events dataset
# specifically the reliance on a list of groups.
def get_project_exclude_groups_key(project_id):
    return "project_exclude_groups:%s" % project_id


def set_project_exclude_groups(project_id, group_ids):
    """Add {group_id: now, ...} to the ZSET for each `group_id` to exclude,
    remove outdated entries based on `settings.REPLACER_KEY_TTL`, and expire
    the entire ZSET incase it's rarely touched."""

    now = time.time()
    key = get_project_exclude_groups_key(project_id)
    p = redis_client.pipeline()

    p.zadd(key, **{str(group_id): now for group_id in group_ids})
    p.zremrangebyscore(key, -1, now - settings.REPLACER_KEY_TTL)
    p.expire(key, int(settings.REPLACER_KEY_TTL))

    p.execute()


def get_project_needs_final_key(project_id):
    return "project_needs_final:%s" % project_id


def set_project_needs_final(project_id):
    return redis_client.set(
        get_project_needs_final_key(project_id), True, ex=settings.REPLACER_KEY_TTL
    )


def get_projects_query_flags(project_ids):
    """\
    1. Fetch `needs_final` for each Project
    2. Fetch groups to exclude for each Project
    3. Trim groups to exclude ZSET for each Project

    Returns (needs_final, group_ids_to_exclude)
    """

    project_ids = set(project_ids)
    now = time.time()
    p = redis_client.pipeline()

    needs_final_keys = [get_project_needs_final_key(project_id) for project_id in project_ids]
    for needs_final_key in needs_final_keys:
        p.get(needs_final_key)

    exclude_groups_keys = [get_project_exclude_groups_key(project_id) for project_id in project_ids]
    for exclude_groups_key in exclude_groups_keys:
        p.zremrangebyscore(exclude_groups_key, float('-inf'), now - settings.REPLACER_KEY_TTL)
        p.zrevrangebyscore(exclude_groups_key, float('inf'), now - settings.REPLACER_KEY_TTL)

    results = p.execute()

    needs_final = any(results[:len(project_ids)])
    exclude_groups = sorted({
        int(group_id) for group_id
        in sum(results[(len(project_ids) + 1)::2], [])
    })

    return (needs_final, exclude_groups)


class EventsReplacerWorker(AbstractBatchWorker):
    """
    A consumer/worker that processes replacements for the events dataset.

    A replacement is a message in kafka describing an action that mutates snuba
    data. We process this action message into replacement event row(s) with new
    values for some columns. These are inserted into Clickhouse and will replace
    the existing rows with the same primary key upon the next OPTIMIZE.
    """
    # TODO should we make this more like the ConsumerWorker ie generic worker that
    # calls out to a replacements processor to interpret the messages and generate
    # the inserts.
    def __init__(self, clickhouse, dataset, metrics=None):
        self.clickhouse = clickhouse
        self.dataset = dataset
        self.metrics = metrics

        self.REQUIRED_COLUMN_NAMES = [col.escaped for col in self.dataset.SCHEMA.REQUIRED_COLUMNS]
        self.ALL_COLUMN_NAMES = [col.escaped for col in self.dataset.SCHEMA.ALL_COLUMNS]

        self.SEEN_MERGE_TXN_CACHE = deque(maxlen=100)

    def process_message(self, message):
        message = json.loads(message.value())
        version = message[0]

        if version == 2:
            type_, event = message[1:3]

            if type_ in ('start_delete_groups', 'start_merge', 'start_unmerge', 'start_delete_tag'):
                return None
            elif type_ == 'end_delete_groups':
                processed = self.process_delete_groups(event)
            elif type_ == 'end_merge':
                processed = self.process_merge(event)
            elif type_ == 'end_unmerge':
                processed = self.process_unmerge(event)
            elif type_ == 'end_delete_tag':
                processed = self.process_delete_tag(event)
            else:
                raise InvalidMessageType("Invalid message type: {}".format(type_))
        else:
            raise InvalidMessageVersion("Unknown message format: " + str(message))

        return processed

    def flush_batch(self, batch):
        for count_query_template, insert_query_template, query_args, query_time_flags in batch:
            query_args.update({'table_name': self.dataset.SCHEMA.QUERY_TABLE})
            count = self.clickhouse.execute_robust(count_query_template % query_args)[0][0]
            if count == 0:
                continue

            # query_time_flags == (type, project_id, [...data...])
            flag_type, project_id = query_time_flags[:2]
            if flag_type == NEEDS_FINAL:
                set_project_needs_final(project_id)
            elif flag_type == EXCLUDE_GROUPS:
                group_ids = query_time_flags[2]
                set_project_exclude_groups(project_id, group_ids)

            t = time.time()
            logger.debug("Executing replace query: %s" % (insert_query_template % query_args))
            self.clickhouse.execute_robust(insert_query_template % query_args)
            duration = int((time.time() - t) * 1000)
            logger.info("Replacing %s rows took %sms" % (count, duration))
            if self.metrics:
                self.metrics.timing('replacements.count', count)
                self.metrics.timing('replacements.duration', duration)

    def shutdown(self):
        pass

    def process_delete_groups(self, message):
        group_ids = message['group_ids']
        if not group_ids:
            return None

        assert all(isinstance(gid, six.integer_types) for gid in group_ids)
        timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
        select_columns = map(lambda i: i if i != 'deleted' else '1', self.REQUIRED_COLUMN_NAMES)

        where = """\
            WHERE project_id = %(project_id)s
            AND group_id IN (%(group_ids)s)
            AND received <= CAST('%(timestamp)s' AS DateTime)
            AND NOT deleted
        """

        count_query_template = """\
            SELECT count()
            FROM %(table_name)s FINAL
        """ + where

        insert_query_template = """\
            INSERT INTO %(table_name)s (%(required_columns)s)
            SELECT %(select_columns)s
            FROM %(table_name)s FINAL
        """ + where

        query_args = {
            'required_columns': ', '.join(self.REQUIRED_COLUMN_NAMES),
            'select_columns': ', '.join(select_columns),
            'project_id': message['project_id'],
            'group_ids': ", ".join(str(gid) for gid in group_ids),
            'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
        }

        query_time_flags = (EXCLUDE_GROUPS, message['project_id'], group_ids)

        return (count_query_template, insert_query_template, query_args, query_time_flags)


    def process_merge(self, message):
        # HACK: We were sending duplicates of the `end_merge` message from Sentry,
        # this is only for performance of the backlog.
        txn = message.get('transaction_id')
        if txn:
            if txn in self.SEEN_MERGE_TXN_CACHE:
                return None
            else:
                self.SEEN_MERGE_TXN_CACHE.append(txn)

        previous_group_ids = message['previous_group_ids']
        if not previous_group_ids:
            return None

        assert all(isinstance(gid, six.integer_types) for gid in previous_group_ids)
        timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
        select_columns = map(lambda i: i if i != 'group_id' else str(message['new_group_id']), self.ALL_COLUMN_NAMES)

        where = """\
            WHERE project_id = %(project_id)s
            AND group_id IN (%(previous_group_ids)s)
            AND received <= CAST('%(timestamp)s' AS DateTime)
            AND NOT deleted
        """

        count_query_template = """\
            SELECT count()
            FROM %(table_name)s FINAL
        """ + where

        insert_query_template = """\
            INSERT INTO %(table_name)s (%(all_columns)s)
            SELECT %(select_columns)s
            FROM %(table_name)s FINAL
        """ + where

        query_args = {
            'all_columns': ', '.join(self.ALL_COLUMN_NAMES),
            'select_columns': ', '.join(select_columns),
            'project_id': message['project_id'],
            'previous_group_ids': ", ".join(str(gid) for gid in previous_group_ids),
            'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
        }

        query_time_flags = (EXCLUDE_GROUPS, message['project_id'], previous_group_ids)

        return (count_query_template, insert_query_template, query_args, query_time_flags)


    def process_unmerge(self, message):
        hashes = message['hashes']
        if not hashes:
            return None

        assert all(isinstance(h, six.string_types) for h in hashes)
        timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
        select_columns = map(lambda i: i if i != 'group_id' else str(message['new_group_id']), self.ALL_COLUMN_NAMES)

        where = """\
            WHERE project_id = %(project_id)s
            AND group_id = %(previous_group_id)s
            AND primary_hash IN (%(hashes)s)
            AND received <= CAST('%(timestamp)s' AS DateTime)
            AND NOT deleted
        """

        count_query_template = """\
            SELECT count()
            FROM %(table_name)s FINAL
        """ + where

        insert_query_template = """\
            INSERT INTO %(table_name)s (%(all_columns)s)
            SELECT %(select_columns)s
            FROM %(table_name)s FINAL
        """ + where

        query_args = {
            'all_columns': ', '.join(self.ALL_COLUMN_NAMES),
            'select_columns': ', '.join(select_columns),
            'previous_group_id': message['previous_group_id'],
            'project_id': message['project_id'],
            'hashes': ", ".join("'%s'" % _hashify(h) for h in hashes),
            'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
        }

        query_time_flags = (NEEDS_FINAL, message['project_id'])

        return (count_query_template, insert_query_template, query_args, query_time_flags)


    def process_delete_tag(self, message):
        tag = message['tag']
        if not tag:
            return None

        assert isinstance(tag, six.string_types)
        timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
        tag_column_name = self.dataset.SCHEMA.TAG_COLUMN_MAP['tags'].get(tag, tag)
        is_promoted = tag in self.dataset.SCHEMA.PROMOTED_TAGS['tags']

        where = """\
            WHERE project_id = %(project_id)s
            AND received <= CAST('%(timestamp)s' AS DateTime)
            AND NOT deleted
        """

        if is_promoted:
            where += "AND %(tag_column)s IS NOT NULL"
        else:
            where += "AND has(`tags.key`, %(tag_str)s)"

        insert_query_template = """\
            INSERT INTO %(table_name)s (%(all_columns)s)
            SELECT %(select_columns)s
            FROM %(table_name)s FINAL
        """ + where

        select_columns = []
        for col in self.dataset.SCHEMA.ALL_COLUMNS:
            if is_promoted and col.flattened == tag_column_name:
                select_columns.append('NULL')
            elif col.flattened == 'tags.key':
                select_columns.append(
                    "arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, %s)), `tags.key`)" % escape_string(tag)
                )
            elif col.flattened == 'tags.value':
                select_columns.append(
                    "arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, %s), arrayEnumerate(`tags.value`)))" % escape_string(tag)
                )
            else:
                select_columns.append(col.escaped)

        query_args = {
            'all_columns': ', '.join(self.ALL_COLUMN_NAMES),
            'select_columns': ', '.join(select_columns),
            'project_id': message['project_id'],
            'tag_str': escape_string(tag),
            'tag_column': escape_col(tag_column_name),
            'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
        }

        count_query_template = """\
            SELECT count()
            FROM %(table_name)s FINAL
        """ + where

        query_time_flags = (NEEDS_FINAL, message['project_id'])

        return (count_query_template, insert_query_template, query_args, query_time_flags)
