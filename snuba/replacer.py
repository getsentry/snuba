import logging
import six
import time
from collections import deque
from datetime import datetime
import simplejson as json

from . import settings
from snuba.consumer import AbstractBatchWorker
from snuba.processor import _hashify, InvalidMessageType, InvalidMessageVersion
from snuba.redis import redis_client
from snuba.util import escape_col


logger = logging.getLogger('snuba.replacer')


CLICKHOUSE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

REQUIRED_COLUMNS = list(map(escape_col, settings.REQUIRED_COLUMNS))
ALL_COLUMNS = list(map(escape_col, settings.WRITER_COLUMNS))

EXCLUDE_GROUPS = object()
NEEDS_FINAL = object()


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
    p.expire(key, int(now + settings.REPLACER_KEY_TTL))

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

    now = time.time()
    p = redis_client.pipeline()

    p.mget({get_project_needs_final_key(project_id) for project_id in project_ids})

    exclude_groups_keys = {get_project_exclude_groups_key(project_id) for project_id in project_ids}
    for exclude_groups_key in exclude_groups_keys:
        p.zrevrangebyscore(exclude_groups_key, float('inf'), now - settings.REPLACER_KEY_TTL)

    for exclude_groups_key in exclude_groups_keys:
        p.zremrangebyscore(exclude_groups_key, float('-inf'), now - settings.REPLACER_KEY_TTL)

    results = p.execute()

    needs_final = any(results[0])
    exclude_groups = sorted({int(group_id) for group_id in sum(results[1:len(project_ids) + 1], [])})

    return (needs_final, exclude_groups)


class ReplacerWorker(AbstractBatchWorker):
    def __init__(self, clickhouse, dist_table_name, metrics=None):
        self.clickhouse = clickhouse
        self.dist_table_name = dist_table_name
        self.metrics = metrics

    def process_message(self, message):
        message = json.loads(message.value())
        version = message[0]

        if version == 2:
            type_, event = message[1:3]

            if type_ in ('start_delete_groups', 'start_merge', 'start_unmerge'):
                return None
            elif type_ == 'end_delete_groups':
                processed = process_delete_groups(event)
            elif type_ == 'end_merge':
                processed = process_merge(event)
            elif type_ == 'end_unmerge':
                processed = process_unmerge(event)
            else:
                raise InvalidMessageType("Invalid message type: {}".format(type_))
        else:
            raise InvalidMessageVersion("Unknown message format: " + str(message))

        return processed

    def flush_batch(self, batch):
        for count_query_template, insert_query_template, query_args, query_time_flags in batch:
            query_args.update({'dist_table_name': self.dist_table_name})
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


def process_delete_groups(message):
    group_ids = message['group_ids']
    if not group_ids:
        return None

    assert all(isinstance(gid, six.integer_types) for gid in group_ids)
    timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
    select_columns = map(lambda i: i if i != 'deleted' else '1', REQUIRED_COLUMNS)

    where = """\
        WHERE project_id = %(project_id)s
        AND group_id IN (%(group_ids)s)
        AND received <= CAST('%(timestamp)s' AS DateTime)
        AND NOT deleted
    """

    count_query_template = """\
        SELECT count()
        FROM %(dist_table_name)s FINAL
    """ + where

    insert_query_template = """\
        INSERT INTO %(dist_table_name)s (%(required_columns)s)
        SELECT %(select_columns)s
        FROM %(dist_table_name)s FINAL
    """ + where

    query_args = {
        'required_columns': ', '.join(REQUIRED_COLUMNS),
        'select_columns': ', '.join(select_columns),
        'project_id': message['project_id'],
        'group_ids': ", ".join(str(gid) for gid in group_ids),
        'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
    }

    query_time_flags = (EXCLUDE_GROUPS, message['project_id'], group_ids)

    return (count_query_template, insert_query_template, query_args, query_time_flags)


SEEN_MERGE_TXN_CACHE = deque(maxlen=100)


def process_merge(message):
    # HACK: We were sending duplicates of the `end_merge` message from Sentry,
    # this is only for performance of the backlog.
    txn = message.get('transaction_id')
    if txn:
        if txn in SEEN_MERGE_TXN_CACHE:
            return None
        else:
            SEEN_MERGE_TXN_CACHE.append(txn)

    previous_group_ids = message['previous_group_ids']
    if not previous_group_ids:
        return None

    assert all(isinstance(gid, six.integer_types) for gid in previous_group_ids)
    timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
    select_columns = map(lambda i: i if i != 'group_id' else str(message['new_group_id']), ALL_COLUMNS)

    where = """\
        WHERE project_id = %(project_id)s
        AND group_id IN (%(previous_group_ids)s)
        AND received <= CAST('%(timestamp)s' AS DateTime)
        AND NOT deleted
    """

    count_query_template = """\
        SELECT count()
        FROM %(dist_table_name)s FINAL
    """ + where

    insert_query_template = """\
        INSERT INTO %(dist_table_name)s (%(all_columns)s)
        SELECT %(select_columns)s
        FROM %(dist_table_name)s FINAL
    """ + where

    query_args = {
        'all_columns': ', '.join(ALL_COLUMNS),
        'select_columns': ', '.join(select_columns),
        'project_id': message['project_id'],
        'previous_group_ids': ", ".join(str(gid) for gid in previous_group_ids),
        'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
    }

    query_time_flags = (EXCLUDE_GROUPS, message['project_id'], previous_group_ids)

    return (count_query_template, insert_query_template, query_args, query_time_flags)


def process_unmerge(message):
    hashes = message['hashes']
    if not hashes:
        return None

    assert all(isinstance(h, six.string_types) for h in hashes)
    timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
    select_columns = map(lambda i: i if i != 'group_id' else str(message['new_group_id']), ALL_COLUMNS)

    where = """\
        WHERE project_id = %(project_id)s
        AND group_id = %(previous_group_id)s
        AND primary_hash IN (%(hashes)s)
        AND received <= CAST('%(timestamp)s' AS DateTime)
        AND NOT deleted
    """

    count_query_template = """\
        SELECT count()
        FROM %(dist_table_name)s FINAL
    """ + where

    insert_query_template = """\
        INSERT INTO %(dist_table_name)s (%(all_columns)s)
        SELECT %(select_columns)s
        FROM %(dist_table_name)s FINAL
    """ + where

    query_args = {
        'all_columns': ', '.join(ALL_COLUMNS),
        'select_columns': ', '.join(select_columns),
        'previous_group_id': message['previous_group_id'],
        'project_id': message['project_id'],
        'hashes': ", ".join("'%s'" % _hashify(h) for h in hashes),
        'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
    }

    query_time_flags = (NEEDS_FINAL, message['project_id'])

    return (count_query_template, insert_query_template, query_args, query_time_flags)
