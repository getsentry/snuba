from __future__ import annotations

import json
import logging
import random
import uuid
from abc import abstractmethod
from collections import deque
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import (
    Any,
    cast,
)

from sentry_kafka_schemas.schema_types.events_v1 import (
    EndDeleteGroupsMessageBody,
    EndDeleteTagMessageBody,
    EndMergeMessageBody,
    EndUnmergeMessageBody,
    ExcludeGroupsMessageBody,
    ReplaceGroupMessageBody,
    TombstoneEventsMessageBody,
)

from snuba import environment, settings
from snuba.clickhouse import DATETIME_FORMAT
from snuba.clickhouse.columns import FlattenedColumn, Nullable, ReadOnly
from snuba.clickhouse.escaping import escape_string
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import (
    REPLACEMENT_EVENT_TYPES,
    InvalidMessageType,
    ReplacementType,
    _hashify,
)
from snuba.replacers.projects_query_flags import ProjectsQueryFlags
from snuba.replacers.replacements_and_expiry import (
    get_config_auto_replacements_bypass_projects,
)
from snuba.replacers.replacer_processor import Replacement as ReplacementBase
from snuba.replacers.replacer_processor import (
    ReplacementMessage,
    ReplacerProcessor,
    ReplacerState,
)
from snuba.state.sentry_options import get_option
from snuba.utils.metrics.wrapper import MetricsWrapper

"""
Disambiguate the dataset/storage when there are multiple tables representing errors
that perform event replacements.
In theory this will be needed only during the events to errors migration.
"""

logger = logging.getLogger(__name__)
metrics = MetricsWrapper(environment.metrics, "errors.replacer")


@dataclass(frozen=True)
class NeedsFinal:
    pass


@dataclass(frozen=True)
class ExcludeGroups:
    group_ids: Sequence[int]


QueryTimeFlags = NeedsFinal | ExcludeGroups


@dataclass(frozen=True)
class ReplacementContext:
    all_columns: Sequence[FlattenedColumn]
    required_columns: Sequence[str]
    state_name: ReplacerState

    tag_column_map: Mapping[str, Mapping[str, str]]
    promoted_tags: Mapping[str, Sequence[str]]
    schema: WritableTableSchema


class Replacement(ReplacementBase):
    @classmethod
    @abstractmethod
    def parse_message(
        cls, message: ReplacementMessage[Any], context: ReplacementContext
    ) -> Replacement | None:
        raise NotImplementedError()

    @abstractmethod
    def get_query_time_flags(self) -> QueryTimeFlags | None:
        raise NotImplementedError()

    @abstractmethod
    def get_project_id(self) -> int:
        raise NotImplementedError()

    def should_write_every_node(self) -> bool:
        write_node_replacement_setting = cast(
            float, get_option("write_node_replacements_global", 1.0)
        )
        return random.random() < write_node_replacement_setting


class ErrorsReplacer(ReplacerProcessor[Replacement]):
    def __init__(
        self,
        required_columns: Sequence[str],
        tag_column_map: Mapping[str, Mapping[str, str]],
        promoted_tags: Mapping[str, Sequence[str]],
        state_name: str,
        storage_key_str: str,
    ) -> None:
        self.__schema: WritableTableSchema | None = None
        self.__required_columns = required_columns
        self.__tag_column_map = tag_column_map
        self.__promoted_tags = promoted_tags
        self.__state_name = ReplacerState(state_name)
        self.__storage_key_str = storage_key_str

    def __initialize_schema(self) -> None:
        # This has to be imported here since the storage factory will also initialize this processor
        # and importing it at the top will create an import cycle

        # This could be avoided by passing the schema in as an argument for this class but that
        # would inflate the config representation of any storage using this replacer
        from snuba.datasets.storages.factory import get_storage

        storage = get_storage(StorageKey(self.__storage_key_str))
        assert isinstance(schema := storage.get_schema(), WritableTableSchema)
        self.__schema = schema
        self.__all_columns = [
            col for col in schema.get_columns() if not col.type.has_modifier(ReadOnly)
        ]
        self.__replacement_context = ReplacementContext(
            all_columns=self.__all_columns,
            state_name=self.__state_name,
            required_columns=self.__required_columns,
            schema=self.__schema,
            tag_column_map=self.__tag_column_map,
            promoted_tags=self.__promoted_tags,
        )

    def process_message(self, message: ReplacementMessage[Mapping[str, Any]]) -> Replacement | None:
        if not self.__schema:
            self.__initialize_schema()
        assert self.__schema is not None

        type_ = message.action_type

        attributes_json = json.dumps({"message_type": type_, **message.data})
        logger.info(attributes_json)

        if type_ in REPLACEMENT_EVENT_TYPES:
            metrics.increment(
                "process",
                1,
                tags={"type": type_, "consumer_group": message.metadata.consumer_group},
            )

        processed: Replacement | None

        if type_ in (
            ReplacementType.START_DELETE_GROUPS,
            ReplacementType.START_MERGE,
            ReplacementType.START_UNMERGE,
            ReplacementType.START_DELETE_TAG,
        ):
            return None
        if type_ in _REPLACEMENT_BY_TYPE:
            processed = _REPLACEMENT_BY_TYPE[type_].parse_message(
                message,
                self.__replacement_context,
            )
        else:
            raise InvalidMessageType(f"Invalid message type: {type_}")

        if processed is not None:
            manual_bypass_projects = cast(str, get_option("replacements_bypass_projects", "[]"))
            auto_bypass_projects = list(
                get_config_auto_replacements_bypass_projects(datetime.now()).keys()
            )
            projects_to_skip = auto_bypass_projects
            if manual_bypass_projects is not None:
                try:
                    projects_to_skip.extend(json.loads(manual_bypass_projects))
                except Exception as e:
                    metrics.increment("errors_replacer_process_message_error")
                    logger.exception(e)
            metrics.gauge("projects_to_skip", value=len(projects_to_skip))
            if processed.get_project_id() in projects_to_skip:
                # For a persistent non rate limited logger
                logger.info(
                    f"Skipping replacement for project. Data {message}, Partition: {message.metadata.partition_index}, Offset: {message.metadata.offset}",
                )
                # For sentry tracking
                logger.error(
                    "Skipping replacement for project",
                    extra={"project_id": processed.get_project_id(), "data": message},
                )
                metrics.increment(
                    "replacement_message_skipped",
                    1,
                    tags={
                        "type": type_,
                        "consumer_group": message.metadata.consumer_group,
                    },
                )
                return None

        return processed

    def get_schema(self) -> WritableTableSchema:
        if not self.__schema:
            self.__initialize_schema()
        assert self.__schema is not None
        return self.__schema

    def get_state(self) -> ReplacerState:
        return self.__state_name

    def pre_replacement(self, replacement: Replacement, matching_records: int) -> bool:
        project_id = replacement.get_project_id()
        query_time_flags = replacement.get_query_time_flags()

        if not settings.REPLACER_IMMEDIATE_OPTIMIZE:
            if isinstance(query_time_flags, NeedsFinal):
                ProjectsQueryFlags.set_project_needs_final(
                    project_id, self.__state_name, replacement.get_replacement_type()
                )

            elif isinstance(query_time_flags, ExcludeGroups):
                ProjectsQueryFlags.set_project_exclude_groups(
                    project_id,
                    query_time_flags.group_ids,
                    self.__state_name,
                    replacement.get_replacement_type(),
                )

        elif query_time_flags is not None:
            return True

        return False


def _build_event_set_filter(
    project_id: int,
    event_ids: Sequence[str],
    from_timestamp: str | None,
    to_timestamp: str | None,
) -> tuple[list[str], list[str], MutableMapping[str, str]]:
    def get_timestamp_condition(msg_value: str | None, operator: str) -> str:
        if not msg_value:
            return ""

        try:
            timestamp = datetime.strptime(msg_value, settings.PAYLOAD_DATETIME_FORMAT)
        except ValueError:  # e.g. "2023-08-28T03:05:38+00:00"
            timestamp = datetime.fromisoformat(msg_value)

        return f"timestamp {operator} toDateTime('{timestamp.strftime(DATETIME_FORMAT)}')"

    from_condition = get_timestamp_condition(from_timestamp, ">=")
    to_condition = get_timestamp_condition(to_timestamp, "<=")

    event_id_lhs = "event_id"
    event_id_list = ", ".join(f"'{uuid.UUID(eid)}'" for eid in event_ids)

    prewhere = [f"{event_id_lhs} IN (%(event_ids)s)"]
    where = ["project_id = %(project_id)s", "NOT deleted"]
    if from_condition:
        where.append(from_condition)
    if to_condition:
        where.append(to_condition)

    query_args = {
        "event_ids": event_id_list,
        "project_id": str(project_id),
    }

    return prewhere, where, query_args


@dataclass
class ReplaceGroupReplacement(Replacement):
    """
    Merge individual events into new group. The old group will have to be
    manually excluded from search queries.

    See docstring of ExcludeGroupsReplacement for an explanation of how this is
    used.

    Note that events merged this way cannot be cleanly unmerged by
    process_unmerge, as their group hashes possibly stand in no correlation to
    how the merging was done.
    """

    event_ids: Sequence[str]
    project_id: int
    from_timestamp: str | None
    to_timestamp: str | None
    new_group_id: int
    all_columns: Sequence[FlattenedColumn]

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[ReplaceGroupMessageBody],
        context: ReplacementContext,
    ) -> ReplaceGroupReplacement | None:
        event_ids = message.data["event_ids"]
        if not event_ids:
            return None

        return cls(
            event_ids=event_ids,
            project_id=message.data["project_id"],
            from_timestamp=message.data.get("from_timestamp"),
            to_timestamp=message.data.get("to_timestamp"),
            new_group_id=message.data["new_group_id"],
            all_columns=context.all_columns,
        )

    def get_project_id(self) -> int:
        return self.project_id

    def get_query_time_flags(self) -> QueryTimeFlags | None:
        return None

    @classmethod
    def get_replacement_type(cls) -> ReplacementType:
        return ReplacementType.REPLACE_GROUP

    @cached_property
    def _where_clause(self) -> str:
        prewhere, where, query_args = _build_event_set_filter(
            project_id=self.project_id,
            event_ids=self.event_ids,
            from_timestamp=self.from_timestamp,
            to_timestamp=self.to_timestamp,
        )

        return f"PREWHERE {' AND '.join(prewhere)} WHERE {' AND '.join(where)}" % query_args

    def get_count_query(self, table_name: str) -> str | None:
        return f"""\
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_insert_query(self, table_name: str) -> str | None:
        all_column_names = [c.escaped for c in self.all_columns]
        select_columns = ", ".join(
            i if i != "group_id" else str(self.new_group_id) for i in all_column_names
        )

        all_columns = ", ".join(all_column_names)

        return f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
            {self._where_clause}
        """


@dataclass
class DeleteGroupsReplacement(Replacement):
    project_id: int
    required_columns: Sequence[str]
    timestamp: datetime
    group_ids: Sequence[int]

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[EndDeleteGroupsMessageBody],
        context: ReplacementContext,
    ) -> DeleteGroupsReplacement | None:
        group_ids = message.data["group_ids"]
        if not group_ids:
            return None

        assert all(isinstance(gid, int) for gid in group_ids)

        timestamp = datetime.strptime(message.data["datetime"], settings.PAYLOAD_DATETIME_FORMAT)

        return cls(
            required_columns=context.required_columns,
            project_id=message.data["project_id"],
            timestamp=timestamp,
            group_ids=group_ids,
        )

    def get_project_id(self) -> int:
        return self.project_id

    def get_query_time_flags(self) -> QueryTimeFlags | None:
        return ExcludeGroups(group_ids=self.group_ids)

    @classmethod
    def get_replacement_type(cls) -> ReplacementType:
        return ReplacementType.END_DELETE_GROUPS

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

    def get_count_query(self, table_name: str) -> str | None:
        return f"""\
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_insert_query(self, table_name: str) -> str | None:
        required_columns = ", ".join(self.required_columns)
        select_columns = ", ".join(i if i != "deleted" else "1" for i in self.required_columns)
        return f"""\
            INSERT INTO {table_name} ({required_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
            {self._where_clause}
        """


@dataclass
class TombstoneEventsReplacement(Replacement):
    event_ids: Sequence[str]
    old_primary_hash: str | None
    project_id: int
    from_timestamp: str | None
    to_timestamp: str | None

    required_columns: Sequence[str]

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[TombstoneEventsMessageBody],
        context: ReplacementContext,
    ) -> TombstoneEventsReplacement | None:
        event_ids = message.data["event_ids"]
        if not event_ids:
            return None

        return cls(
            project_id=message.data["project_id"],
            event_ids=event_ids,
            old_primary_hash=message.data.get("old_primary_hash"),
            from_timestamp=message.data.get("from_timestamp"),
            to_timestamp=message.data.get("to_timestamp"),
            required_columns=context.required_columns,
        )

    @cached_property
    def _where_clause(self) -> str:
        prewhere, where, query_args = _build_event_set_filter(
            project_id=self.project_id,
            event_ids=self.event_ids,
            from_timestamp=self.from_timestamp,
            to_timestamp=self.to_timestamp,
        )

        if self.old_primary_hash:
            query_args["old_primary_hash"] = f"'{str(uuid.UUID(self.old_primary_hash))}'"

            prewhere.append("primary_hash = %(old_primary_hash)s")

        return f"PREWHERE {' AND '.join(prewhere)} WHERE {' AND '.join(where)}" % query_args

    def get_count_query(self, table_name: str) -> str | None:
        return f"""\
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_insert_query(self, table_name: str) -> str | None:
        required_columns = ", ".join(self.required_columns)
        select_columns = ", ".join(i if i != "deleted" else "1" for i in self.required_columns)

        return f"""\
            INSERT INTO {table_name} ({required_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_query_time_flags(self) -> QueryTimeFlags | None:
        return None

    def get_project_id(self) -> int:
        return self.project_id

    @classmethod
    def get_replacement_type(cls) -> ReplacementType:
        return ReplacementType.TOMBSTONE_EVENTS


@dataclass
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
        cls,
        message: ReplacementMessage[ExcludeGroupsMessageBody],
        context: ReplacementContext,
    ) -> ExcludeGroupsReplacement | None:
        if not message.data["group_ids"]:
            return None

        return cls(
            project_id=message.data["project_id"],
            group_ids=message.data["group_ids"],
        )

    def get_project_id(self) -> int:
        return self.project_id

    def get_query_time_flags(self) -> QueryTimeFlags | None:
        return ExcludeGroups(group_ids=self.group_ids)

    @classmethod
    def get_replacement_type(cls) -> ReplacementType:
        return ReplacementType.EXCLUDE_GROUPS

    def get_insert_query(self, table_name: str) -> str | None:
        return None

    def get_count_query(self, table_name: str) -> str | None:
        return None


SEEN_MERGE_TXN_CACHE: deque[str] = deque(maxlen=100)


@dataclass
class MergeReplacement(Replacement):
    """
    Merge all events of one group into another group.

    The old group ID should not receive new events, as the group ID will be
    excluded from queries and the new events will not be able to be queried.

    This is roughly equivalent to sending:

        process_merge_events (for each event)
        process_exclude_groups
    """

    project_id: int
    previous_group_ids: Sequence[int]
    new_group_id: int
    new_group_first_seen: datetime | None
    timestamp: datetime

    all_columns: Sequence[FlattenedColumn]

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[EndMergeMessageBody],
        context: ReplacementContext,
    ) -> MergeReplacement | None:
        project_id = message.data["project_id"]
        previous_group_ids = message.data["previous_group_ids"]
        if not previous_group_ids:
            return None

        assert all(isinstance(gid, int) for gid in previous_group_ids)

        timestamp = datetime.strptime(message.data["datetime"], settings.PAYLOAD_DATETIME_FORMAT)

        # HACK: We were sending duplicates of the `end_merge` message from Sentry,
        # this is only for performance of the backlog.
        txn = message.data.get("transaction_id")
        if txn:
            if txn in SEEN_MERGE_TXN_CACHE:
                logger.error(
                    "Skipping duplicate group replacement",
                    extra={"project_id": project_id},
                )
                return None
            SEEN_MERGE_TXN_CACHE.append(txn)

        # new_group_first_seen was added to the message schema; keep this check
        # for backwards compatibility.
        raw_new_group_first_seen = message.data.get("new_group_first_seen")
        if raw_new_group_first_seen:
            new_group_first_seen = datetime.strptime(
                raw_new_group_first_seen, settings.PAYLOAD_DATETIME_FORMAT
            )
        else:
            new_group_first_seen = None

        return cls(
            project_id=project_id,
            previous_group_ids=previous_group_ids,
            new_group_id=message.data["new_group_id"],
            timestamp=timestamp,
            new_group_first_seen=new_group_first_seen,
            all_columns=context.all_columns,
        )

    @cached_property
    def _where_clause(self) -> str:
        previous_group_ids = ", ".join(str(gid) for gid in self.previous_group_ids)
        timestamp = self.timestamp.strftime(DATETIME_FORMAT)

        return f"""\
            PREWHERE group_id IN ({previous_group_ids})
            WHERE project_id = {self.project_id}
            AND received <= CAST('{timestamp}' AS DateTime)
            AND NOT deleted
        """

    def get_count_query(self, table_name: str) -> str | None:
        return f"""\
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_insert_query(self, table_name: str) -> str | None:
        all_column_names = [c.escaped for c in self.all_columns]
        all_columns = ", ".join(all_column_names)
        replacement_columns = {"group_id": str(self.new_group_id)}

        if self.new_group_first_seen is not None:
            group_first_seen_str = self.new_group_first_seen.strftime(DATETIME_FORMAT)
            replacement_columns["group_first_seen"] = f"CAST('{group_first_seen_str}' AS DateTime)"

        select_columns = ", ".join(replacement_columns.get(i, i) for i in all_column_names)
        return f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_query_time_flags(self) -> QueryTimeFlags:
        return ExcludeGroups(group_ids=self.previous_group_ids)

    def get_project_id(self) -> int:
        return self.project_id

    @classmethod
    def get_replacement_type(cls) -> ReplacementType:
        return ReplacementType.END_MERGE


@dataclass(frozen=True)
class UnmergeGroupsReplacement(Replacement):
    state_name: ReplacerState
    timestamp: datetime
    hashes: Sequence[str]
    all_columns: Sequence[FlattenedColumn]
    project_id: int
    previous_group_id: int
    new_group_id: int

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[EndUnmergeMessageBody],
        context: ReplacementContext,
    ) -> Replacement | None:
        hashes = message.data["hashes"]
        if not hashes:
            return None

        assert all(isinstance(h, str) for h in hashes)

        timestamp = datetime.strptime(message.data["datetime"], settings.PAYLOAD_DATETIME_FORMAT)

        return UnmergeGroupsReplacement(
            state_name=context.state_name,
            timestamp=timestamp,
            hashes=hashes,
            project_id=message.data["project_id"],
            previous_group_id=message.data["previous_group_id"],
            new_group_id=message.data["new_group_id"],
            all_columns=context.all_columns,
        )

    def get_project_id(self) -> int:
        return self.project_id

    def get_query_time_flags(self) -> QueryTimeFlags | None:
        return NeedsFinal()

    @classmethod
    def get_replacement_type(cls) -> ReplacementType:
        return ReplacementType.END_UNMERGE

    @cached_property
    def _where_clause(self) -> str:
        if self.state_name == ReplacerState.ERRORS:
            hashes = ", ".join([f"'{str(uuid.UUID(_hashify(h)))}'" for h in self.hashes])
        else:
            hashes = ", ".join(f"'{_hashify(h)}'" for h in self.hashes)

        timestamp = self.timestamp.strftime(DATETIME_FORMAT)

        return f"""\
            PREWHERE primary_hash IN ({hashes})
            WHERE group_id = {self.previous_group_id}
            AND project_id = {self.project_id}
            AND received <= CAST('{timestamp}' AS DateTime)
            AND NOT deleted
        """

    def get_count_query(self, table_name: str) -> str | None:
        return f"""\
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_insert_query(self, table_name: str) -> str | None:
        all_column_names = [c.escaped for c in self.all_columns]
        select_columns = ", ".join(
            i if i != "group_id" else str(self.new_group_id) for i in all_column_names
        )

        all_columns = ", ".join(all_column_names)

        return f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
            {self._where_clause}
        """


def _convert_hash(hash: str, state_name: ReplacerState, convert_types: bool = False) -> str:
    if state_name == ReplacerState.ERRORS:
        if convert_types:
            return f"toUUID('{str(uuid.UUID(_hashify(hash)))}')"
        return f"'{str(uuid.UUID(_hashify(hash)))}'"
    if convert_types:
        return f"toFixedString('{_hashify(hash)}', 32)"
    return f"'{_hashify(hash)}'"


@dataclass
class DeleteTagReplacement(Replacement):
    project_id: int
    tag: str
    timestamp: datetime
    tag_column_name: str
    is_promoted: bool
    all_columns: Sequence[FlattenedColumn]
    schema: WritableTableSchema

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[EndDeleteTagMessageBody],
        context: ReplacementContext,
    ) -> DeleteTagReplacement | None:
        tag = message.data["tag"]
        if not tag:
            return None

        assert isinstance(tag, str)
        timestamp = datetime.strptime(message.data["datetime"], settings.PAYLOAD_DATETIME_FORMAT)
        tag_column_name = context.tag_column_map["tags"].get(tag, tag)
        is_promoted = tag in context.promoted_tags["tags"]

        return cls(
            project_id=message.data["project_id"],
            tag=tag,
            timestamp=timestamp,
            tag_column_name=tag_column_name,
            is_promoted=is_promoted,
            all_columns=context.all_columns,
            schema=context.schema,
        )

    @cached_property
    def _where_clause(self) -> str:
        tag_str = escape_string(self.tag)
        timestamp = self.timestamp.strftime(DATETIME_FORMAT)
        return f"""\
            WHERE project_id = {self.project_id}
            AND received <= CAST('{timestamp}' AS DateTime)
            AND NOT deleted
            AND has(`tags.key`, {tag_str})
        """

    @cached_property
    def _select_columns(self) -> Sequence[str]:
        select_columns = []
        for col in self.all_columns:
            if self.is_promoted and col.flattened == self.tag_column_name:
                # The promoted tag columns of events are non nullable, but those of
                # errors are non nullable. We check the column against the schema
                # to determine whether to write an empty string or NULL.
                column_type = self.schema.get_data_source().get_columns().get(self.tag_column_name)
                assert column_type is not None
                is_nullable = column_type.type.has_modifier(Nullable)
                if is_nullable:
                    select_columns.append("NULL")
                else:
                    select_columns.append("''")
            elif col.flattened == "tags.key":
                select_columns.append(
                    f"arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, {escape_string(self.tag)})), `tags.key`)"
                )
            elif col.flattened == "tags.value":
                select_columns.append(
                    f"arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, {escape_string(self.tag)}), arrayEnumerate(`tags.value`)))"
                )
            else:
                select_columns.append(col.escaped)

        return select_columns

    def get_insert_query(self, table_name: str) -> str | None:
        all_columns = ", ".join(col.escaped for col in self.all_columns)
        select_columns = ", ".join(self._select_columns)

        return f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_count_query(self, table_name: str) -> str | None:
        return f"""\
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_query_time_flags(self) -> QueryTimeFlags:
        return NeedsFinal()

    def get_project_id(self) -> int:
        return self.project_id

    @classmethod
    def get_replacement_type(cls) -> ReplacementType:
        return ReplacementType.END_DELETE_TAG


_REPLACEMENT_BY_TYPE: Mapping[ReplacementType, type[Replacement]] = {
    cls.get_replacement_type(): cls
    for cls in cast(
        Sequence[type[Replacement]],
        [
            DeleteGroupsReplacement,
            MergeReplacement,
            UnmergeGroupsReplacement,
            DeleteTagReplacement,
            TombstoneEventsReplacement,
            ReplaceGroupReplacement,
            ExcludeGroupsReplacement,
        ],
    )
}
