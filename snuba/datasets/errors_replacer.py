import logging
import time
import uuid
from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Any, Deque, Mapping, Optional, Sequence, Tuple

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


class ReplacerState(Enum):
    """
    Disambiguate the dataset/storage when there are multiple tables representing errors
    that perform event replacements.
    In theory this will be needed only during the events to errors migration.
    """

    EVENTS = "events"
    ERRORS = "errors"


@dataclass
class ReplacementContext:
    all_columns: Sequence[FlattenedColumn]
    required_columns: Sequence[str]
    state_name: ReplacerState

    tag_column_map: Mapping[str, Mapping[str, str]]
    promoted_tags: Mapping[str, Sequence[str]]
    use_promoted_prewhere: bool
    schema: WritableTableSchema


class Replacement(ReplacementBase):
    @abstractmethod
    def get_excluded_groups(self) -> Sequence[int]:
        raise NotImplementedError()

    @abstractmethod
    def get_needs_final(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_project_id(self) -> int:
        raise NotImplementedError()


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
        self.__replacement_context = ReplacementContext(
            all_columns=self.__all_columns,
            state_name=self.__state_name,
            required_columns=self.__required_columns,
            use_promoted_prewhere=self.__use_promoted_prewhere,
            schema=self.__schema,
            tag_column_map=self.__tag_column_map,
            promoted_tags=self.__promoted_tags,
        )

    def process_message(self, message: ReplacementMessage) -> Optional[Replacement]:
        type_ = message.action_type
        event = message.data
        context = self.__replacement_context

        if type_ in (
            "start_delete_groups",
            "start_merge",
            "start_unmerge",
            "start_delete_tag",
        ):
            return None
        elif type_ == "end_delete_groups":
            processed = DeleteGroupsReplacement.parse_message(event, context)
        elif type_ == "end_merge":
            processed = MergeGroupsReplacement.parse_message(event, context)
        elif type_ == "end_unmerge":
            processed = UnmergeReplacement.parse_message(event, context)
        elif type_ == "end_delete_tag":
            processed = DeleteTagReplacement.parse_message(event, context)
        elif type_ == "tombstone_events":
            processed = TombstoneEventsReplacement.parse_message(event, context)
        elif type_ == "replace_group":
            processed = ReplaceGroupReplacement.parse_message(event, context)
        elif type_ == "exclude_groups":
            processed = ExcludeGroupsReplacement.parse_message(event, context)
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

        needs_final = replacement.get_needs_final()
        group_ids_to_exclude = replacement.get_excluded_groups()
        project_id = replacement.get_project_id()

        if not settings.REPLACER_IMMEDIATE_OPTIMIZE:
            if needs_final:
                if compatibility_double_write:
                    set_project_needs_final(project_id, None)
                set_project_needs_final(project_id, self.__state_name)

            if group_ids_to_exclude:
                if compatibility_double_write:
                    set_project_exclude_groups(project_id, group_ids_to_exclude, None)
                set_project_exclude_groups(
                    project_id, group_ids_to_exclude, self.__state_name
                )

        elif needs_final or group_ids_to_exclude:
            return True

        return False


@dataclass(frozen=True)
class _TombstoneMixin:
    required_columns: Sequence[str]

    @cached_property
    def _where_clause(self) -> str:
        raise NotImplementedError()

    def get_insert_query(self, table_name: str) -> Optional[str]:
        required_columns = ", ".join(self.required_columns)
        select_columns = ", ".join(
            map(lambda i: i if i != "deleted" else "1", self.required_columns)
        )

        return (
            f"""\
           INSERT INTO {table_name} ({required_columns})
           SELECT {select_columns}
           FROM {table_name} FINAL
           """
            + self._where_clause
        )

    def get_count_query(self, table_name: str) -> Optional[str]:
        return (
            f"""\
            SELECT count()
            FROM {table_name} FINAL
        """
            + self._where_clause
        )


@dataclass(frozen=True)
class _EventSetFilterMixin:
    project_id: int
    event_ids: Sequence[str]
    from_timestamp: Optional[str]
    to_timestamp: Optional[str]
    state_name: ReplacerState

    def _get_event_set_filter(self,) -> Tuple[str, str]:
        def get_timestamp_condition(value: Optional[str], operator: str) -> str:
            if not value:
                return ""

            timestamp = datetime.strptime(value, settings.PAYLOAD_DATETIME_FORMAT)
            return f"timestamp {operator} toDateTime('{timestamp.strftime(DATETIME_FORMAT)}')"

        from_condition = get_timestamp_condition(self.from_timestamp, ">=")
        to_condition = get_timestamp_condition(self.to_timestamp, "<=")

        if self.state_name == ReplacerState.EVENTS:
            event_id_lhs = "cityHash64(toString(event_id))"
            event_id_list = ", ".join(
                [
                    f"cityHash64('{str(uuid.UUID(event_id)).replace('-', '')}')"
                    for event_id in self.event_ids
                ]
            )
        else:
            event_id_lhs = "event_id"
            event_id_list = ", ".join("'%s'" % uuid.UUID(eid) for eid in self.event_ids)

        prewhere = [f"{event_id_lhs} IN (%(event_ids)s)"]
        where = ["project_id = %(project_id)s", "NOT deleted"]
        if from_condition:
            where.append(from_condition)
        if to_condition:
            where.append(to_condition)

        query_args = {
            "event_ids": event_id_list,
            "project_id": self.project_id,
        }

        return " AND ".join(prewhere) % query_args, " AND ".join(where) % query_args


@dataclass(frozen=True)
class ReplaceGroupReplacement(Replacement, _EventSetFilterMixin):
    """
    Merge individual events into new group. The old group will have to be
    manually excluded from search queries.

    See docstring of ExcludeGroupsReplacement for an explanation of how this is used.

    Note that events merged this way cannot be cleanly unmerged by
    process_unmerge, as their group hashes possibly stand in no correlation to
    how the merging was done.
    """

    new_group_id: int
    all_column_names: Sequence[str]

    def get_project_id(self) -> int:
        return self.project_id

    @classmethod
    def parse_message(
        cls, message: Mapping[str, Any], context: ReplacementContext
    ) -> Optional["ReplaceGroupReplacement"]:
        txn = message.get("transaction_id")

        # HACK: We were sending duplicates of the `end_merge` message from Sentry,
        # this is only for performance of the backlog.
        if txn:
            if txn in SEEN_MERGE_TXN_CACHE:
                return None
            else:
                SEEN_MERGE_TXN_CACHE.append(txn)

        event_ids = message["event_ids"]
        if not event_ids:
            return None

        new_group_id = message["new_group_id"]
        project_id: int = message["project_id"]
        all_column_names = [c.escaped for c in context.all_columns]

        return cls(
            project_id=project_id,
            new_group_id=new_group_id,
            event_ids=event_ids,
            from_timestamp=message.get("from_timestamp"),
            to_timestamp=message.get("to_timestamp"),
            state_name=context.state_name,
            all_column_names=all_column_names,
        )

    def get_excluded_groups(self) -> Sequence[int]:
        return []

    def get_needs_final(self) -> bool:
        return False

    @cached_property
    def _where_clause(self) -> str:
        prewhere, where = self._get_event_set_filter()
        return f"PREWHERE {prewhere} WHERE {where}"

    def get_count_query(self, table_name: str) -> Optional[str]:
        where = self._where_clause

        return (
            f"""\
            SELECT count()
            FROM {table_name} FINAL
        """
            + where
        )

    def get_insert_query(self, table_name: str) -> Optional[str]:
        where = self._where_clause

        all_columns = ", ".join(self.all_column_names)
        select_columns = ", ".join(
            map(
                lambda i: i if i != "group_id" else str(self.new_group_id),
                self.all_column_names,
            )
        )

        return (
            f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
        """
            + where
        )


@dataclass(frozen=True)
class DeleteGroupsReplacement(_TombstoneMixin, Replacement):
    group_ids: Sequence[int]
    timestamp: datetime
    project_id: int

    @classmethod
    def parse_message(
        cls, message: Mapping[str, Any], context: ReplacementContext
    ) -> Optional["Replacement"]:
        group_ids = message["group_ids"]
        if not group_ids:
            return None

        assert all(isinstance(gid, int) for gid in group_ids)
        timestamp = datetime.strptime(
            message["datetime"], settings.PAYLOAD_DATETIME_FORMAT
        )

        return DeleteGroupsReplacement(
            required_columns=context.required_columns,
            group_ids=group_ids,
            timestamp=timestamp,
            project_id=message["project_id"],
        )

    def get_excluded_groups(self) -> Sequence[int]:
        return self.group_ids

    def get_needs_final(self) -> bool:
        return False

    def get_project_id(self) -> int:
        return self.project_id

    @cached_property
    def _where_clause(self) -> str:
        group_ids = ", ".join(str(gid) for gid in self.group_ids)
        timestamp = self.timestamp.strftime(DATETIME_FORMAT)

        return f"""\
            PREWHERE group_id IN ({group_ids})
            WHERE project_id = {self.project_id}
            AND received <= CAST('{timestamp}' AS DateTime)
            AND NOT deleted
        """


@dataclass(frozen=True)
class TombstoneEventsReplacement(_EventSetFilterMixin, _TombstoneMixin, Replacement):
    old_primary_hash: Optional[str]

    @classmethod
    def parse_message(
        cls, message: Mapping[str, Any], context: ReplacementContext
    ) -> Optional["Replacement"]:
        event_ids = message["event_ids"]
        if not event_ids:
            return None

        old_primary_hash = message.get("old_primary_hash")

        if old_primary_hash and context.state_name == ReplacerState.EVENTS:
            # old_primary_hash flag means the event is only tombstoned
            # because it will be reinserted with a changed primary_hash. Since
            # primary_hash is part of the sortkey/primarykey in the ERRORS table,
            # we need to tombstone the old event. In the old EVENTS table we do
            # not.
            return None

        return TombstoneEventsReplacement(
            required_columns=context.required_columns,
            event_ids=event_ids,
            from_timestamp=message.get("from_timestamp"),
            to_timestamp=message.get("to_timestamp"),
            project_id=message["project_id"],
            state_name=context.state_name,
            old_primary_hash=old_primary_hash,
        )

    def get_excluded_groups(self) -> Sequence[int]:
        return []

    def get_needs_final(self) -> bool:
        return False

    def get_project_id(self) -> int:
        return self.project_id

    @cached_property
    def _where_clause(self) -> str:
        prewhere, where = self._get_event_set_filter()

        primary_hash_cond = ""

        if self.old_primary_hash:
            old_primary_hash_fmt = (
                ("'%s'" % (str(uuid.UUID(self.old_primary_hash)),))
                if self.old_primary_hash
                else "NULL"
            )

            primary_hash_cond = f" AND primary_hash = {old_primary_hash_fmt}"

        return f"PREWHERE {prewhere}{primary_hash_cond} WHERE {where}"


@dataclass(frozen=True)
class ExcludeGroupsReplacement(Replacement):
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

    project_id: int
    group_ids: Sequence[int]

    @classmethod
    def parse_message(
        cls, message: Mapping[str, Any], context: ReplacementContext
    ) -> Optional["ExcludeGroupsReplacement"]:
        if not message["group_ids"]:
            return None

        return cls(message["project_id"], message["group_ids"])

    def get_project_id(self) -> int:
        return self.project_id

    def get_excluded_groups(self) -> Sequence[int]:
        return self.group_ids

    def get_needs_final(self) -> bool:
        return False

    def get_insert_query(self, table_name: str) -> Optional[str]:
        return None

    def get_count_query(self, table_name: str) -> Optional[str]:
        return None


SEEN_MERGE_TXN_CACHE: Deque[str] = deque(maxlen=100)


@dataclass(frozen=True)
class MergeGroupsReplacement(Replacement):
    """
    Merge all events of one group into another group.

    The old group ID should not receive new events, as the group ID will be
    excluded from queries and the new events will not be able to be queried.

    This is roughly equivalent to sending:

        process_merge_events (for each event)
        ExcludeGroupsReplacement
    """

    timestamp: datetime
    project_id: int
    previous_group_ids: Sequence[int]
    new_group_id: int
    all_column_names: Sequence[str]

    @classmethod
    def parse_message(
        cls, message: Mapping[str, Any], context: ReplacementContext
    ) -> Optional["MergeGroupsReplacement"]:
        txn = message.get("transaction_id")

        # HACK: We were sending duplicates of the `end_merge` message from Sentry,
        # this is only for performance of the backlog.
        if txn:
            if txn in SEEN_MERGE_TXN_CACHE:
                return None
            else:
                SEEN_MERGE_TXN_CACHE.append(txn)

        previous_group_ids = message["previous_group_ids"]
        if not previous_group_ids:
            return None

        assert all(isinstance(gid, int) for gid in previous_group_ids)

        timestamp = datetime.strptime(
            message["datetime"], settings.PAYLOAD_DATETIME_FORMAT
        )
        project_id: int = message["project_id"]
        all_column_names = [c.escaped for c in context.all_columns]

        return cls(
            project_id=project_id,
            previous_group_ids=previous_group_ids,
            timestamp=timestamp,
            new_group_id=message["new_group_id"],
            all_column_names=all_column_names,
        )

    def get_needs_final(self) -> bool:
        return False

    def get_project_id(self) -> int:
        return self.project_id

    def get_excluded_groups(self) -> Sequence[int]:
        return self.previous_group_ids

    @cached_property
    def _where_clause(self) -> str:
        previous_group_ids = ", ".join(str(gid) for gid in self.previous_group_ids)
        ts = self.timestamp.strftime(DATETIME_FORMAT)

        return f"""\
            PREWHERE group_id IN ({previous_group_ids})
            WHERE project_id = {self.project_id}
            AND received <= CAST('{ts}' AS DateTime)
            AND NOT deleted
        """

    def get_count_query(self, table_name: str) -> Optional[str]:
        where = self._where_clause

        return (
            f"""\
            SELECT count()
            FROM {table_name} FINAL
        """
            + where
        )

    def get_insert_query(self, table_name: str) -> Optional[str]:
        where = self._where_clause

        all_columns = ", ".join(self.all_column_names)
        select_columns = ", ".join(
            map(
                lambda i: i if i != "group_id" else str(self.new_group_id),
                self.all_column_names,
            )
        )

        return (
            f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
        """
            + where
        )


@dataclass(frozen=True)
class UnmergeReplacement(Replacement):
    state_name: ReplacerState
    timestamp: datetime
    hashes: Sequence[str]
    all_columns: Sequence[FlattenedColumn]
    project_id: int
    previous_group_id: int
    new_group_id: int

    @classmethod
    def parse_message(
        cls, message: Mapping[str, Any], context: ReplacementContext
    ) -> Optional["Replacement"]:
        hashes = message["hashes"]
        if not hashes:
            return None

        assert all(isinstance(h, str) for h in hashes)

        timestamp = datetime.strptime(
            message["datetime"], settings.PAYLOAD_DATETIME_FORMAT
        )

        return UnmergeReplacement(
            state_name=context.state_name,
            timestamp=timestamp,
            hashes=hashes,
            project_id=message["project_id"],
            previous_group_id=message["previous_group_id"],
            new_group_id=message["new_group_id"],
            all_columns=context.all_columns,
        )

    def get_excluded_groups(self) -> Sequence[int]:
        return []

    def get_needs_final(self) -> bool:
        return True

    def get_project_id(self) -> int:
        return self.project_id

    @cached_property
    def _where_clause(self) -> str:
        if self.state_name == ReplacerState.ERRORS:
            hashes = ", ".join(
                ["'%s'" % str(uuid.UUID(_hashify(h))) for h in self.hashes]
            )
        else:
            hashes = ", ".join("'%s'" % _hashify(h) for h in self.hashes)

        timestamp = self.timestamp.strftime(DATETIME_FORMAT)

        return f"""\
            PREWHERE group_id = {self.previous_group_id}
            WHERE project_id = {self.project_id}
            AND primary_hash IN ({hashes})
            AND received <= CAST('{timestamp}' AS DateTime)
            AND NOT deleted
        """

    def get_count_query(self, table_name: str) -> Optional[str]:
        return (
            f"""\
            SELECT count()
            FROM {table_name} FINAL
        """
            + self._where_clause
        )

    def get_insert_query(self, table_name: str) -> Optional[str]:
        all_column_names = [c.escaped for c in self.all_columns]
        select_columns = ", ".join(
            map(
                lambda i: i if i != "group_id" else str(self.new_group_id),
                all_column_names,
            )
        )

        all_columns = ", ".join(all_column_names)

        return (
            f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
        """
            + self._where_clause
        )


@dataclass(frozen=True)
class DeleteTagReplacement(Replacement):
    tag: str
    project_id: int
    timestamp: datetime
    is_promoted: bool
    use_promoted_prewhere: bool
    tag_column_name: str
    tag_column_is_nullable: bool
    all_columns: Sequence[FlattenedColumn]

    @classmethod
    def parse_message(
        cls, message: Mapping[str, Any], context: ReplacementContext
    ) -> Optional["Replacement"]:
        tag = message["tag"]
        if not tag:
            return None

        assert isinstance(tag, str)
        timestamp = datetime.strptime(
            message["datetime"], settings.PAYLOAD_DATETIME_FORMAT
        )
        tag_column_name = context.tag_column_map["tags"].get(tag, tag)
        is_promoted = tag in context.promoted_tags["tags"]
        column_type = (
            context.schema.get_data_source().get_columns().get(tag_column_name)
        )
        tag_column_is_nullable = (
            column_type is not None and column_type.type.has_modifier(Nullable)
        )

        return cls(
            tag=tag,
            timestamp=timestamp,
            tag_column_name=tag_column_name,
            is_promoted=is_promoted,
            use_promoted_prewhere=context.use_promoted_prewhere,
            tag_column_is_nullable=tag_column_is_nullable,
            project_id=message["project_id"],
            all_columns=context.all_columns,
        )

    @cached_property
    def _select_columns(self) -> Sequence[str]:
        select_columns = []
        for col in self.all_columns:
            if self.is_promoted and col.flattened == self.tag_column_name:
                # The promoted tag columns of events are non nullable, but those of
                # errors are non nullable. We check the column against the schema
                # to determine whether to write an empty string or NULL.
                if self.tag_column_is_nullable:
                    select_columns.append("NULL")
                else:
                    select_columns.append("''")
            elif col.flattened == "tags.key":
                select_columns.append(
                    "arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, %s)), `tags.key`)"
                    % escape_string(self.tag)
                )
            elif col.flattened == "tags.value":
                select_columns.append(
                    "arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, %s), arrayEnumerate(`tags.value`)))"
                    % escape_string(self.tag)
                )
            else:
                select_columns.append(col.escaped)

        return select_columns

    @cached_property
    def _where_clause(self) -> str:
        timestamp = self.timestamp.strftime(DATETIME_FORMAT)

        if self.is_promoted and self.use_promoted_prewhere:
            tag_column = escape_identifier(self.tag_column_name)
            prewhere = f" PREWHERE {tag_column} IS NOT NULL "
        else:
            tag_str = escape_string(self.tag)
            prewhere = f" PREWHERE has(`tags.key`, {tag_str}) "

        return f"""\
            {prewhere}
            WHERE project_id = {self.project_id}
            AND received <= CAST('{timestamp}' AS DateTime)
            AND NOT deleted
        """

    def get_insert_query(self, table_name: str) -> Optional[str]:
        all_column_names = [col.escaped for col in self.all_columns]
        all_columns = ", ".join(all_column_names)
        select_columns = ", ".join(self._select_columns)
        return (
            f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
        """
            + self._where_clause
        )

    def get_count_query(self, table_name: str) -> Optional[str]:
        return (
            f"""\
            SELECT count()
            FROM {table_name} FINAL
        """
            + self._where_clause
        )

    def get_needs_final(self) -> bool:
        return True

    def get_excluded_groups(self) -> Sequence[int]:
        return []

    def get_project_id(self) -> int:
        return self.project_id
