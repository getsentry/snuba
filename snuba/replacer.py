import logging
import six
import time
from collections import deque
from datetime import datetime
import simplejson as json

from . import settings
from snuba.consumer import AbstractBatchWorker
from snuba.processor import _hashify, InvalidMessageType, InvalidMessageVersion
from snuba.util import escape_col


logger = logging.getLogger('snuba.replacer')


CLICKHOUSE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

REQUIRED_COLUMNS = list(map(escape_col, settings.REQUIRED_COLUMNS))
ALL_COLUMNS = list(map(escape_col, settings.WRITER_COLUMNS))


class ReplacerWorker(AbstractBatchWorker):
    def __init__(self, clickhouse, dist_table_name, metrics=None):
        self.clickhouse = clickhouse
        self.dist_table_name = dist_table_name
        self.metrics = metrics

    def process_message(self, message):
        message = json.loads(message.value())

        try:
            version = message[0]
        except KeyError:
            # this message was produced erroneously during backfill because some commits from
            # master were (temporarily) not in the backfill branch. skip this event because
            # we are going to replay the backfill from those times in order to generate correct events
            return None

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
        for count_query_template, insert_query_template, args in batch:
            args.update({'dist_table_name': self.dist_table_name})
            count = self.clickhouse.execute_robust(count_query_template % args)[0][0]

            if count == 0:
                continue

            t = time.time()
            logger.debug("Executing replace query: %s" % (insert_query_template % args))
            self.clickhouse.execute_robust(insert_query_template % args)
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

    return (count_query_template, insert_query_template, query_args)


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

    return (count_query_template, insert_query_template, query_args)


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

    return (count_query_template, insert_query_template, query_args)
