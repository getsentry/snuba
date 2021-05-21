import logging
import time
import uuid
from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    Deque,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from snuba import settings
from snuba.clickhouse import DATETIME_FORMAT
from snuba.clickhouse.columns import FlattenedColumn, Nullable, ReadOnly
from snuba.clickhouse.escaping import escape_identifier, escape_string
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.processor import InvalidMessageType, _hashify
from snuba.redis import redis_client
from snuba.replacers.replacer_processor import Replacement as ReplacementBase
from snuba.replacers.replacer_processor import ReplacementMessage, ReplacerProcessor

logger = logging.getLogger(__name__)

"""
Disambiguate the dataset/storage when there are multiple tables representing errors
that perform event replacements.
In theory this will be needed only during the events to errors migration.
"""


class ReplacerState(Enum):
    EVENTS = "events"
    ERRORS = "errors"


@dataclass(frozen=True)
class NeedsFinal:
    pass


@dataclass(frozen=True)
class ExcludeGroups:
    group_ids: Sequence[int]


QueryTimeFlags = Union[NeedsFinal, ExcludeGroups]


class Replacement(ReplacementBase):
    @abstractmethod
    def get_query_time_flags(self) -> Optional[QueryTimeFlags]:
        raise NotImplementedError()

    @abstractmethod
    def get_project_id(self) -> int:
        raise NotImplementedError()


EXCLUDE_GROUPS = object()
NEEDS_FINAL = object()
LegacyQueryTimeFlags = Union[Tuple[object, int], Tuple[object, int, Any]]


@dataclass(frozen=True)
class LegacyReplacement(Replacement):
    # XXX: For the group_exclude message we need to be able to run a
    # replacement without running any query.
    count_query_template: Optional[str]
    insert_query_template: Optional[str]
    query_args: Mapping[str, Any]
    query_time_flags: LegacyQueryTimeFlags

    def get_project_id(self) -> int:
        return self.query_time_flags[1]

    def get_query_time_flags(self) -> Optional[QueryTimeFlags]:
        if self.query_time_flags[0] == NEEDS_FINAL:
            return NeedsFinal()

        if self.query_time_flags[0] == EXCLUDE_GROUPS:
            return ExcludeGroups(group_ids=self.query_time_flags[2])  # type: ignore

        return None

    def get_insert_query(self, table_name: str) -> Optional[str]:
        if self.insert_query_template is None:
            return None

        args = {**self.query_args, "table_name": table_name}
        return self.insert_query_template % args

    def get_count_query(self, table_name: str) -> Optional[str]:
        if self.count_query_template is None:
            return None

        args = {**self.query_args, "table_name": table_name}
        return self.count_query_template % args


def get_project_exclude_groups_key(
    project_id: int, state_name: Optional[ReplacerState]
) -> str:
    return f"project_exclude_groups:{f'{state_name.value}:' if state_name else ''}{project_id}"


def set_project_exclude_groups(
    project_id: int, group_ids: Sequence[int], state_name: Optional[ReplacerState]
) -> None:
    """Add {group_id: now, ...} to the ZSET for each `group_id` to exclude,
    remove outdated entries based on `settings.REPLACER_KEY_TTL`, and expire
    the entire ZSET incase it's rarely touched."""

    now = time.time()
    key = get_project_exclude_groups_key(project_id, state_name)
    p = redis_client.pipeline()

    group_id_data: Mapping[str, float] = {str(group_id): now for group_id in group_ids}
    p.zadd(key, **group_id_data)
    p.zremrangebyscore(key, -1, now - settings.REPLACER_KEY_TTL)
    p.expire(key, int(settings.REPLACER_KEY_TTL))

    p.execute()


def get_project_needs_final_key(
    project_id: int, state_name: Optional[ReplacerState]
) -> str:
    return f"project_needs_final:{f'{state_name.value}:' if state_name else ''}{project_id}"


def set_project_needs_final(
    project_id: int, state_name: Optional[ReplacerState]
) -> Optional[bool]:
    return redis_client.set(
        get_project_needs_final_key(project_id, state_name),
        True,
        ex=settings.REPLACER_KEY_TTL,
    )


def get_projects_query_flags(
    project_ids: Sequence[int], state_name: Optional[ReplacerState]
) -> Tuple[bool, Sequence[int]]:
    """\
    1. Fetch `needs_final` for each Project
    2. Fetch groups to exclude for each Project
    3. Trim groups to exclude ZSET for each Project

    Returns (needs_final, group_ids_to_exclude)
    """

    s_project_ids = set(project_ids)
    now = time.time()
    p = redis_client.pipeline()

    needs_final_keys = [
        get_project_needs_final_key(project_id, state_name)
        for project_id in s_project_ids
    ]
    for needs_final_key in needs_final_keys:
        p.get(needs_final_key)

    exclude_groups_keys = [
        get_project_exclude_groups_key(project_id, state_name)
        for project_id in s_project_ids
    ]
    for exclude_groups_key in exclude_groups_keys:
        p.zremrangebyscore(
            exclude_groups_key, float("-inf"), now - settings.REPLACER_KEY_TTL
        )
        p.zrevrangebyscore(
            exclude_groups_key, float("inf"), now - settings.REPLACER_KEY_TTL
        )

    results = p.execute()

    needs_final = any(results[: len(s_project_ids)])
    exclude_groups = sorted(
        {int(group_id) for group_id in sum(results[(len(s_project_ids) + 1) :: 2], [])}
    )

    return (needs_final, exclude_groups)


class ErrorsReplacer(ReplacerProcessor[Replacement]):
    def __init__(
        self,
        schema: WritableTableSchema,
        required_columns: Sequence[str],
        tag_column_map: Mapping[str, Mapping[str, str]],
        promoted_tags: Mapping[str, Sequence[str]],
        state_name: ReplacerState,
        use_promoted_prewhere: bool,
    ) -> None:
        super().__init__(schema=schema)
        self.__required_columns = required_columns
        self.__all_columns = [
            col for col in schema.get_columns() if not col.type.has_modifier(ReadOnly)
        ]

        self.__tag_column_map = tag_column_map
        self.__promoted_tags = promoted_tags
        self.__state_name = state_name
        self.__use_promoted_prewhere = use_promoted_prewhere
        self.__schema = schema

    def process_message(self, message: ReplacementMessage) -> Optional[Replacement]:
        type_ = message.action_type
        event = message.data

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
            processed = process_merge(event, self.__all_columns)
        elif type_ == "end_unmerge":
            processed = process_unmerge(event, self.__all_columns, self.__state_name)
        elif type_ == "end_delete_tag":
            processed = process_delete_tag(
                event,
                self.__all_columns,
                self.__tag_column_map,
                self.__promoted_tags,
                self.__use_promoted_prewhere,
                self.__schema,
            )
        elif type_ == "tombstone_events":
            processed = process_tombstone_events(
                event, self.__required_columns, self.__state_name
            )
        elif type_ == "replace_group":
            processed = process_replace_group(
                event, self.__all_columns, self.__state_name
            )
        elif type_ == "exclude_groups":
            processed = process_exclude_groups(event)
        else:
            raise InvalidMessageType("Invalid message type: {}".format(type_))

        return processed

    def pre_replacement(self, replacement: Replacement, matching_records: int) -> bool:
        if self.__state_name == ReplacerState.EVENTS:
            # Backward compatibility with the old keys already in Redis, we will let double write
            # the old key structure and the new one for a while then we can get rid of the old one.
            compatibility_double_write = True
        else:
            compatibility_double_write = False

        project_id = replacement.get_project_id()
        query_time_flags = replacement.get_query_time_flags()

        if not settings.REPLACER_IMMEDIATE_OPTIMIZE:
            if isinstance(query_time_flags, NeedsFinal):
                if compatibility_double_write:
                    set_project_needs_final(project_id, None)
                set_project_needs_final(project_id, self.__state_name)

            elif isinstance(query_time_flags, ExcludeGroups):
                if compatibility_double_write:
                    set_project_exclude_groups(
                        project_id, query_time_flags.group_ids, None
                    )
                set_project_exclude_groups(
                    project_id, query_time_flags.group_ids, self.__state_name
                )

        elif query_time_flags is not None:
            return True

        return False


def _build_event_tombstone_replacement(
    message: Mapping[str, Any],
    required_columns: Sequence[str],
    where: str,
    query_args: Mapping[str, str],
    query_time_flags: LegacyQueryTimeFlags,
) -> Replacement:
    select_columns = map(lambda i: i if i != "deleted" else "1", required_columns)
    count_query_template = (
        """\
        SELECT count()
        FROM %(table_name)s FINAL
    """
        + where
    )

    insert_query_template = (
        """\
        INSERT INTO %(table_name)s (%(required_columns)s)
        SELECT %(select_columns)s
        FROM %(table_name)s FINAL
    """
        + where
    )

    final_query_args = {
        "required_columns": ", ".join(required_columns),
        "select_columns": ", ".join(select_columns),
        "project_id": message["project_id"],
    }
    final_query_args.update(query_args)

    return LegacyReplacement(
        count_query_template, insert_query_template, final_query_args, query_time_flags
    )


def _build_group_replacement(
    txn: Optional[str],
    project_id: int,
    new_group_id: str,
    where: str,
    query_args: Mapping[str, str],
    query_time_flags: LegacyQueryTimeFlags,
    all_columns: Sequence[FlattenedColumn],
) -> Optional[Replacement]:
    # HACK: We were sending duplicates of the `end_merge` message from Sentry,
    # this is only for performance of the backlog.
    if txn:
        if txn in SEEN_MERGE_TXN_CACHE:
            return None
        else:
            SEEN_MERGE_TXN_CACHE.append(txn)

    all_column_names = [c.escaped for c in all_columns]
    select_columns = map(
        lambda i: i if i != "group_id" else str(new_group_id), all_column_names,
    )

    count_query_template = (
        """\
        SELECT count()
        FROM %(table_name)s FINAL
    """
        + where
    )

    insert_query_template = (
        """\
        INSERT INTO %(table_name)s (%(all_columns)s)
        SELECT %(select_columns)s
        FROM %(table_name)s FINAL
    """
        + where
    )

    final_query_args = {
        "all_columns": ", ".join(all_column_names),
        "select_columns": ", ".join(select_columns),
        "project_id": project_id,
    }
    final_query_args.update(query_args)

    return LegacyReplacement(
        count_query_template, insert_query_template, final_query_args, query_time_flags
    )


def _build_event_set_filter(
    message: Mapping[str, Any], state_name: ReplacerState
) -> Optional[Tuple[List[str], List[str], MutableMapping[str, str]]]:
    event_ids = message["event_ids"]
    if not event_ids:
        return None

    def get_timestamp_condition(msg_field: str, operator: str) -> str:
        msg_value = message.get(msg_field)
        if not msg_value:
            return ""

        timestamp = datetime.strptime(msg_value, settings.PAYLOAD_DATETIME_FORMAT)
        return (
            f"timestamp {operator} toDateTime('{timestamp.strftime(DATETIME_FORMAT)}')"
        )

    from_condition = get_timestamp_condition("from_timestamp", ">=")
    to_condition = get_timestamp_condition("to_timestamp", "<=")

    if state_name == ReplacerState.EVENTS:
        event_id_lhs = "cityHash64(toString(event_id))"
        event_id_list = ", ".join(
            [
                f"cityHash64('{str(uuid.UUID(event_id)).replace('-', '')}')"
                for event_id in event_ids
            ]
        )
    else:
        event_id_lhs = "event_id"
        event_id_list = ", ".join("'%s'" % uuid.UUID(eid) for eid in event_ids)

    prewhere = [f"{event_id_lhs} IN (%(event_ids)s)"]
    where = ["project_id = %(project_id)s", "NOT deleted"]
    if from_condition:
        where.append(from_condition)
    if to_condition:
        where.append(to_condition)

    query_args = {
        "event_ids": event_id_list,
        "project_id": message["project_id"],
    }

    return prewhere, where, query_args


def process_replace_group(
    message: Mapping[str, Any],
    all_columns: Sequence[FlattenedColumn],
    state_name: ReplacerState,
) -> Optional[Replacement]:
    """
    Merge individual events into new group. The old group will have to be
    manually excluded from search queries.

    See docstring of process_exclude_groups for an explanation of how this is used.

    Note that events merged this way cannot be cleanly unmerged by
    process_unmerge, as their group hashes possibly stand in no correlation to
    how the merging was done.
    """

    event_ids = message["event_ids"]
    if not event_ids:
        return None

    event_set_filter = _build_event_set_filter(message, state_name)
    if event_set_filter is None:
        return None

    prewhere, where, query_args = event_set_filter

    full_where = f"PREWHERE {' AND '.join(prewhere)} WHERE {' AND '.join(where)}"
    project_id: int = message["project_id"]
    query_time_flags = (None, project_id)

    return _build_group_replacement(
        message.get("transaction_id"),
        project_id,
        message["new_group_id"],
        full_where,
        query_args,
        query_time_flags,
        all_columns,
    )


def process_delete_groups(
    message: Mapping[str, Any], required_columns: Sequence[str]
) -> Optional[Replacement]:
    group_ids = message["group_ids"]
    if not group_ids:
        return None

    assert all(isinstance(gid, int) for gid in group_ids)
    timestamp = datetime.strptime(message["datetime"], settings.PAYLOAD_DATETIME_FORMAT)

    where = """\
        PREWHERE group_id IN (%(group_ids)s)
        WHERE project_id = %(project_id)s
        AND received <= CAST('%(timestamp)s' AS DateTime)
        AND NOT deleted
    """

    query_args = {
        "group_ids": ", ".join(str(gid) for gid in group_ids),
        "timestamp": timestamp.strftime(DATETIME_FORMAT),
    }

    query_time_flags = (EXCLUDE_GROUPS, message["project_id"], group_ids)

    return _build_event_tombstone_replacement(
        message, required_columns, where, query_args, query_time_flags
    )


def process_tombstone_events(
    message: Mapping[str, Any],
    required_columns: Sequence[str],
    state_name: ReplacerState,
) -> Optional[Replacement]:
    event_ids = message["event_ids"]
    if not event_ids:
        return None

    old_primary_hash = message.get("old_primary_hash")

    if old_primary_hash and state_name == ReplacerState.EVENTS:
        # old_primary_hash flag means the event is only tombstoned
        # because it will be reinserted with a changed primary_hash. Since
        # primary_hash is part of the sortkey/primarykey in the ERRORS table,
        # we need to tombstone the old event. In the old EVENTS table we do
        # not.
        return None

    event_set_filter = _build_event_set_filter(message, state_name)
    if event_set_filter is None:
        return None

    prewhere, where, query_args = event_set_filter

    if old_primary_hash:
        try:
            parsed_hash = uuid.UUID(old_primary_hash)
        except Exception as err:
            logger.error("Invalid old primary hash %s", old_primary_hash, exc_info=err)
            return None

        query_args["old_primary_hash"] = f"'{str(parsed_hash)}'"

        prewhere.append("primary_hash = %(old_primary_hash)s")

    query_time_flags = (None, message["project_id"])

    full_where = f"PREWHERE {' AND '.join(prewhere)} WHERE {' AND '.join(where)}"

    return _build_event_tombstone_replacement(
        message, required_columns, full_where, query_args, query_time_flags
    )


def process_exclude_groups(message: Mapping[str, Any]) -> Optional[Replacement]:
    """
    Exclude a group ID from being searched.

    This together with process_tombstone_events and process_merge_events is
    used by reprocessing to split up a group into multiple, event by event.
    Assuming a group with n events:

    1. insert m events that have been selected for reprocessing (with same event ID).
    2. process_merge_events for n - m events that have not been selected, i.e.
       move them into a new group ID
    3. exclude old group ID from search queries. This group ID must not receive
       new events.

    See docstring in `sentry.reprocessing2` for more information.
    """

    group_ids = message["group_ids"]
    if not group_ids:
        return None

    query_time_flags = (EXCLUDE_GROUPS, message["project_id"], group_ids)
    return LegacyReplacement(None, None, {}, query_time_flags)


SEEN_MERGE_TXN_CACHE: Deque[str] = deque(maxlen=100)


def process_merge(
    message: Mapping[str, Any], all_columns: Sequence[FlattenedColumn]
) -> Optional[Replacement]:
    """
    Merge all events of one group into another group.

    The old group ID should not receive new events, as the group ID will be
    excluded from queries and the new events will not be able to be queried.

    This is roughly equivalent to sending:

        process_merge_events (for each event)
        process_exclude_groups
    """

    where = """\
        PREWHERE group_id IN (%(previous_group_ids)s)
        WHERE project_id = %(project_id)s
        AND received <= CAST('%(timestamp)s' AS DateTime)
        AND NOT deleted
    """

    previous_group_ids = message["previous_group_ids"]
    if not previous_group_ids:
        return None

    assert all(isinstance(gid, int) for gid in previous_group_ids)

    timestamp = datetime.strptime(message["datetime"], settings.PAYLOAD_DATETIME_FORMAT)

    query_args = {
        "previous_group_ids": ", ".join(str(gid) for gid in previous_group_ids),
        "timestamp": timestamp.strftime(DATETIME_FORMAT),
    }

    project_id: int = message["project_id"]
    query_time_flags = (EXCLUDE_GROUPS, project_id, previous_group_ids)

    return _build_group_replacement(
        message.get("transaction_id"),
        project_id,
        message["new_group_id"],
        where,
        query_args,
        query_time_flags,
        all_columns,
    )


def process_unmerge(
    message: Mapping[str, Any],
    all_columns: Sequence[FlattenedColumn],
    state_name: ReplacerState,
) -> Optional[Replacement]:
    hashes = message["hashes"]
    if not hashes:
        return None

    assert all(isinstance(h, str) for h in hashes)

    timestamp = datetime.strptime(message["datetime"], settings.PAYLOAD_DATETIME_FORMAT)
    all_column_names = [c.escaped for c in all_columns]
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
        FROM %(table_name)s FINAL
    """
        + where
    )

    insert_query_template = (
        """\
        INSERT INTO %(table_name)s (%(all_columns)s)
        SELECT %(select_columns)s
        FROM %(table_name)s FINAL
    """
        + where
    )

    query_args = {
        "all_columns": ", ".join(all_column_names),
        "select_columns": ", ".join(select_columns),
        "previous_group_id": message["previous_group_id"],
        "project_id": message["project_id"],
        "timestamp": timestamp.strftime(DATETIME_FORMAT),
    }

    if state_name == ReplacerState.ERRORS:
        query_args["hashes"] = ", ".join(
            ["'%s'" % str(uuid.UUID(_hashify(h))) for h in hashes]
        )
    else:
        query_args["hashes"] = ", ".join("'%s'" % _hashify(h) for h in hashes)

    query_time_flags = (NEEDS_FINAL, message["project_id"])

    return LegacyReplacement(
        count_query_template, insert_query_template, query_args, query_time_flags
    )


def process_delete_tag(
    message: Mapping[str, Any],
    all_columns: Sequence[FlattenedColumn],
    tag_column_map: Mapping[str, Mapping[str, str]],
    promoted_tags: Mapping[str, Sequence[str]],
    use_promoted_prewhere: bool,
    schema: WritableTableSchema,
) -> Optional[Replacement]:
    tag = message["tag"]
    if not tag:
        return None

    assert isinstance(tag, str)
    timestamp = datetime.strptime(message["datetime"], settings.PAYLOAD_DATETIME_FORMAT)
    tag_column_name = tag_column_map["tags"].get(tag, tag)
    is_promoted = tag in promoted_tags["tags"]

    where = """\
        WHERE project_id = %(project_id)s
        AND received <= CAST('%(timestamp)s' AS DateTime)
        AND NOT deleted
    """

    if is_promoted and use_promoted_prewhere:
        prewhere = " PREWHERE %(tag_column)s IS NOT NULL "
    else:
        prewhere = " PREWHERE has(`tags.key`, %(tag_str)s) "

    insert_query_template = (
        """\
        INSERT INTO %(table_name)s (%(all_columns)s)
        SELECT %(select_columns)s
        FROM %(table_name)s FINAL
    """
        + prewhere
        + where
    )

    select_columns = []
    for col in all_columns:
        if is_promoted and col.flattened == tag_column_name:
            # The promoted tag columns of events are non nullable, but those of
            # errors are non nullable. We check the column against the schema
            # to determine whether to write an empty string or NULL.
            column_type = schema.get_data_source().get_columns().get(tag_column_name)
            assert column_type is not None
            is_nullable = column_type.type.has_modifier(Nullable)
            if is_nullable:
                select_columns.append("NULL")
            else:
                select_columns.append("''")
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
        FROM %(table_name)s FINAL
    """
        + prewhere
        + where
    )

    query_time_flags = (NEEDS_FINAL, message["project_id"])

    return LegacyReplacement(
        count_query_template, insert_query_template, query_args, query_time_flags
    )
