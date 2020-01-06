import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

import simplejson as json

from snuba.clickhouse import DATETIME_FORMAT
from snuba.clickhouse.escaping import escape_identifier, escape_string
from snuba.clickhouse.native import ClickhousePool
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import enforce_table_writer
from snuba.processor import InvalidMessageType, InvalidMessageVersion, _hashify
from snuba.redis import redis_client
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.streams.batching import AbstractBatchWorker
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message

from . import settings


logger = logging.getLogger("snuba.replacer")


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

    needs_final_keys = [
        get_project_needs_final_key(project_id) for project_id in project_ids
    ]
    for needs_final_key in needs_final_keys:
        p.get(needs_final_key)

    exclude_groups_keys = [
        get_project_exclude_groups_key(project_id) for project_id in project_ids
    ]
    for exclude_groups_key in exclude_groups_keys:
        p.zremrangebyscore(
            exclude_groups_key, float("-inf"), now - settings.REPLACER_KEY_TTL
        )
        p.zrevrangebyscore(
            exclude_groups_key, float("inf"), now - settings.REPLACER_KEY_TTL
        )

    results = p.execute()

    needs_final = any(results[: len(project_ids)])
    exclude_groups = sorted(
        {int(group_id) for group_id in sum(results[(len(project_ids) + 1) :: 2], [])}
    )

    return (needs_final, exclude_groups)


@dataclass(frozen=True)
class Replacement:
    count_query_template: str
    insert_query_template: str
    query_args: Mapping[str, Any]
    query_time_flags: Any


class ReplacerWorker(AbstractBatchWorker[KafkaPayload, Replacement]):
    def __init__(
        self, clickhouse: ClickhousePool, dataset: Dataset, metrics: MetricsBackend
    ) -> None:
        self.clickhouse = clickhouse
        self.dataset = dataset
        self.metrics = metrics
        self.__all_column_names = [
            col.escaped
            for col in enforce_table_writer(dataset).get_schema().get_columns()
        ]
        self.__required_columns = [
            col.escaped for col in dataset.get_required_columns()
        ]

    def process_message(self, message: Message[KafkaPayload]) -> Optional[Replacement]:
        message = json.loads(message.payload.value)
        version = message[0]

        if version == 2:
            type_, event = message[1:3]

            if type_ in (
                "start_delete_groups",
                "start_merge",
                "start_unmerge",
                "start_delete_tag",
            ):
                return None
            elif type_ == "end_delete_groups":
                processed = process_delete_groups(event, self.__required_columns)
            elif type_ == "end_merge":
                processed = process_merge(event, self.__all_column_names)
            elif type_ == "end_unmerge":
                processed = process_unmerge(event, self.__all_column_names)
            elif type_ == "end_delete_tag":
                processed = process_delete_tag(event, self.dataset)
            else:
                raise InvalidMessageType("Invalid message type: {}".format(type_))
        else:
            raise InvalidMessageVersion("Unknown message format: " + str(message))

        return processed

    def flush_batch(self, batch: Sequence[Replacement]) -> None:
        for replacement in batch:
            query_args = {
                **replacement.query_args,
                "dist_read_table_name": self.dataset.get_dataset_schemas()
                .get_read_schema()
                .get_data_source()
                .format_from(),
                "dist_write_table_name": enforce_table_writer(self.dataset)
                .get_schema()
                .get_table_name(),
            }
            count = self.clickhouse.execute_robust(
                replacement.count_query_template % query_args
            )[0][0]
            if count == 0:
                continue

            # query_time_flags == (type, project_id, [...data...])
            flag_type, project_id = replacement.query_time_flags[:2]
            if flag_type == NEEDS_FINAL:
                set_project_needs_final(project_id)
            elif flag_type == EXCLUDE_GROUPS:
                group_ids = replacement.query_time_flags[2]
                set_project_exclude_groups(project_id, group_ids)

            t = time.time()
            query = replacement.insert_query_template % query_args
            logger.debug("Executing replace query: %s" % query)
            self.clickhouse.execute_robust(query)
            duration = int((time.time() - t) * 1000)
            logger.info("Replacing %s rows took %sms" % (count, duration))
            self.metrics.timing("replacements.count", count)
            self.metrics.timing("replacements.duration", duration)


def process_delete_groups(message, required_columns) -> Optional[Replacement]:
    group_ids = message["group_ids"]
    if not group_ids:
        return None

    assert all(isinstance(gid, int) for gid in group_ids)
    timestamp = datetime.strptime(message["datetime"], settings.PAYLOAD_DATETIME_FORMAT)
    select_columns = map(lambda i: i if i != "deleted" else "1", required_columns)

    where = """\
        PREWHERE group_id IN (%(group_ids)s)
        WHERE project_id = %(project_id)s
        AND received <= CAST('%(timestamp)s' AS DateTime)
        AND NOT deleted
    """

    count_query_template = (
        """\
        SELECT count()
        FROM %(dist_read_table_name)s FINAL
    """
        + where
    )

    insert_query_template = (
        """\
        INSERT INTO %(dist_write_table_name)s (%(required_columns)s)
        SELECT %(select_columns)s
        FROM %(dist_read_table_name)s FINAL
    """
        + where
    )

    query_args = {
        "required_columns": ", ".join(required_columns),
        "select_columns": ", ".join(select_columns),
        "project_id": message["project_id"],
        "group_ids": ", ".join(str(gid) for gid in group_ids),
        "timestamp": timestamp.strftime(DATETIME_FORMAT),
    }

    query_time_flags = (EXCLUDE_GROUPS, message["project_id"], group_ids)

    return Replacement(
        count_query_template, insert_query_template, query_args, query_time_flags
    )


SEEN_MERGE_TXN_CACHE = deque(maxlen=100)


def process_merge(message, all_column_names) -> Optional[Replacement]:
    # HACK: We were sending duplicates of the `end_merge` message from Sentry,
    # this is only for performance of the backlog.
    txn = message.get("transaction_id")
    if txn:
        if txn in SEEN_MERGE_TXN_CACHE:
            return None
        else:
            SEEN_MERGE_TXN_CACHE.append(txn)

    previous_group_ids = message["previous_group_ids"]
    if not previous_group_ids:
        return None

    assert all(isinstance(gid, int) for gid in previous_group_ids)
    timestamp = datetime.strptime(message["datetime"], settings.PAYLOAD_DATETIME_FORMAT)
    select_columns = map(
        lambda i: i if i != "group_id" else str(message["new_group_id"]),
        all_column_names,
    )

    where = """\
        PREWHERE group_id IN (%(previous_group_ids)s)
        WHERE project_id = %(project_id)s
        AND received <= CAST('%(timestamp)s' AS DateTime)
        AND NOT deleted
    """

    count_query_template = (
        """\
        SELECT count()
        FROM %(dist_read_table_name)s FINAL
    """
        + where
    )

    insert_query_template = (
        """\
        INSERT INTO %(dist_write_table_name)s (%(all_columns)s)
        SELECT %(select_columns)s
        FROM %(dist_read_table_name)s FINAL
    """
        + where
    )

    query_args = {
        "all_columns": ", ".join(all_column_names),
        "select_columns": ", ".join(select_columns),
        "project_id": message["project_id"],
        "previous_group_ids": ", ".join(str(gid) for gid in previous_group_ids),
        "timestamp": timestamp.strftime(DATETIME_FORMAT),
    }

    query_time_flags = (EXCLUDE_GROUPS, message["project_id"], previous_group_ids)

    return Replacement(
        count_query_template, insert_query_template, query_args, query_time_flags
    )


def process_unmerge(message, all_column_names) -> Optional[Replacement]:
    hashes = message["hashes"]
    if not hashes:
        return None

    assert all(isinstance(h, str) for h in hashes)
    timestamp = datetime.strptime(message["datetime"], settings.PAYLOAD_DATETIME_FORMAT)
    select_columns = map(
        lambda i: i if i != "group_id" else str(message["new_group_id"]),
        all_column_names,
    )

    where = """\
        PREWHERE group_id = %(previous_group_id)s
        WHERE project_id = %(project_id)s
        AND primary_hash IN (%(hashes)s)
        AND received <= CAST('%(timestamp)s' AS DateTime)
        AND NOT deleted
    """

    count_query_template = (
        """\
        SELECT count()
        FROM %(dist_read_table_name)s FINAL
    """
        + where
    )

    insert_query_template = (
        """\
        INSERT INTO %(dist_write_table_name)s (%(all_columns)s)
        SELECT %(select_columns)s
        FROM %(dist_read_table_name)s FINAL
    """
        + where
    )

    query_args = {
        "all_columns": ", ".join(all_column_names),
        "select_columns": ", ".join(select_columns),
        "previous_group_id": message["previous_group_id"],
        "project_id": message["project_id"],
        "hashes": ", ".join("'%s'" % _hashify(h) for h in hashes),
        "timestamp": timestamp.strftime(DATETIME_FORMAT),
    }

    query_time_flags = (NEEDS_FINAL, message["project_id"])

    return Replacement(
        count_query_template, insert_query_template, query_args, query_time_flags
    )


def process_delete_tag(message, dataset) -> Optional[Replacement]:
    tag = message["tag"]
    if not tag:
        return None

    assert isinstance(tag, str)
    timestamp = datetime.strptime(message["datetime"], settings.PAYLOAD_DATETIME_FORMAT)
    tag_column_name = dataset.get_tag_column_map()["tags"].get(tag, tag)
    is_promoted = tag in dataset.get_promoted_tags()["tags"]

    where = """\
        WHERE project_id = %(project_id)s
        AND received <= CAST('%(timestamp)s' AS DateTime)
        AND NOT deleted
    """

    if is_promoted:
        where += "AND %(tag_column)s IS NOT NULL"
    else:
        where += "AND has(`tags.key`, %(tag_str)s)"

    insert_query_template = (
        """\
        INSERT INTO %(dist_write_table_name)s (%(all_columns)s)
        SELECT %(select_columns)s
        FROM %(dist_read_table_name)s FINAL
    """
        + where
    )

    select_columns = []
    all_columns = dataset.get_dataset_schemas().get_read_schema().get_columns()
    for col in all_columns:
        if is_promoted and col.flattened == tag_column_name:
            select_columns.append("NULL")
        elif col.flattened == "tags.key":
            select_columns.append(
                "arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, %s)), `tags.key`)"
                % escape_string(tag)
            )
        elif col.flattened == "tags.value":
            select_columns.append(
                "arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, %s), arrayEnumerate(`tags.value`)))"
                % escape_string(tag)
            )
        else:
            select_columns.append(col.escaped)

    all_column_names = [col.escaped for col in all_columns]
    query_args = {
        "all_columns": ", ".join(all_column_names),
        "select_columns": ", ".join(select_columns),
        "project_id": message["project_id"],
        "tag_str": escape_string(tag),
        "tag_column": escape_identifier(tag_column_name),
        "timestamp": timestamp.strftime(DATETIME_FORMAT),
    }

    count_query_template = (
        """\
        SELECT count()
        FROM %(dist_read_table_name)s FINAL
    """
        + where
    )

    query_time_flags = (NEEDS_FINAL, message["project_id"])

    return Replacement(
        count_query_template, insert_query_template, query_args, query_time_flags
    )
