from __future__ import annotations

import json
import logging
import random
import uuid
from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
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
    cast,
)

from sentry_kafka_schemas.schema_types.events_v1 import (
    EndDeleteGroupsMessageBody,
    EndDeleteTagMessageBody,
    EndMergeMessageBody,
    EndUnmergeHierarchicalMessageBody,
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
from snuba.replacers.replacer_processor import Replacement as ReplacementBase
from snuba.replacers.replacer_processor import (
    ReplacementMessage,
    ReplacementMessageMetadata,
    ReplacerProcessor,
    ReplacerState,
)
from snuba.state import get_config
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


QueryTimeFlags = Union[NeedsFinal, ExcludeGroups]


@dataclass(frozen=True)
class ReplacementContext:
    all_columns: Sequence[FlattenedColumn]
    required_columns: Sequence[str]
    state_name: ReplacerState

    tag_column_map: Mapping[str, Mapping[str, str]]
    promoted_tags: Mapping[str, Sequence[str]]
    schema: WritableTableSchema


class Replacement(ReplacementBase):
    @abstractmethod
    def get_query_time_flags(self) -> Optional[QueryTimeFlags]:
        raise NotImplementedError()

    @abstractmethod
    def get_project_id(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def get_replacement_type(self) -> ReplacementType:
        raise NotImplementedError()

    @abstractmethod
    def get_message_metadata(self) -> ReplacementMessageMetadata:
        raise NotImplementedError()

    def should_write_every_node(self) -> bool:
        project_rollout_setting = get_config("write_node_replacements_projects", "")
        if project_rollout_setting:
            # The expected for mat is [project,project,...]
            project_rollout_setting = project_rollout_setting[1:-1]
            if project_rollout_setting:
                rolled_out_projects = [
                    int(p.strip()) for p in project_rollout_setting.split(",")
                ]
                if self.get_project_id() in rolled_out_projects:
                    return True

        global_rollout_setting = get_config("write_node_replacements_global", 0.0)
        assert isinstance(global_rollout_setting, float)
        if random.random() < global_rollout_setting:
            return True

        return False


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
    replacement_type: ReplacementType
    replacement_message_metadata: ReplacementMessageMetadata

    def get_project_id(self) -> int:
        return self.query_time_flags[1]

    def get_query_time_flags(self) -> Optional[QueryTimeFlags]:
        if self.query_time_flags[0] == NEEDS_FINAL:
            return NeedsFinal()

        if self.query_time_flags[0] == EXCLUDE_GROUPS:
            return ExcludeGroups(group_ids=self.query_time_flags[2])  # type: ignore

        return None

    def get_replacement_type(self) -> ReplacementType:
        return self.replacement_type

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

    def get_message_metadata(self) -> ReplacementMessageMetadata:
        return self.replacement_message_metadata


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

    def process_message(
        self, message: ReplacementMessage[Mapping[str, Any]]
    ) -> Optional[Replacement]:
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

        processed: Optional[Replacement]

        if type_ in (
            ReplacementType.START_DELETE_GROUPS,
            ReplacementType.START_MERGE,
            ReplacementType.START_UNMERGE,
            ReplacementType.START_UNMERGE_HIERARCHICAL,
            ReplacementType.START_DELETE_TAG,
        ):
            return None
        elif type_ == ReplacementType.END_DELETE_GROUPS:
            processed = DeleteGroupsReplacement.parse_message(
                cast(ReplacementMessage[EndDeleteGroupsMessageBody], message),
                self.__replacement_context,
            )
        elif type_ == ReplacementType.END_MERGE:
            processed = process_merge(
                cast(ReplacementMessage[EndMergeMessageBody], message),
                self.__all_columns,
            )
        elif type_ == ReplacementType.END_UNMERGE:
            processed = UnmergeGroupsReplacement.parse_message(
                cast(ReplacementMessage[EndUnmergeMessageBody], message),
                self.__replacement_context,
            )
        elif type_ == ReplacementType.END_UNMERGE_HIERARCHICAL:
            processed = UnmergeHierarchicalReplacement.parse_message(
                cast(ReplacementMessage[EndUnmergeHierarchicalMessageBody], message),
                self.__replacement_context,
            )
        elif type_ == ReplacementType.END_DELETE_TAG:
            processed = DeleteTagReplacement.parse_message(
                cast(ReplacementMessage[EndDeleteTagMessageBody], message),
                self.__replacement_context,
            )
        elif type_ == ReplacementType.TOMBSTONE_EVENTS:
            processed = process_tombstone_events(
                cast(ReplacementMessage[TombstoneEventsMessageBody], message),
                self.__required_columns,
                self.__state_name,
            )
        elif type_ == ReplacementType.REPLACE_GROUP:
            processed = ReplaceGroupReplacement.parse_message(
                cast(ReplacementMessage[ReplaceGroupMessageBody], message),
                self.__replacement_context,
            )
        elif type_ == ReplacementType.EXCLUDE_GROUPS:
            processed = ExcludeGroupsReplacement.parse_message(
                cast(ReplacementMessage[ExcludeGroupsMessageBody], message),
                self.__replacement_context,
            )
        else:
            raise InvalidMessageType("Invalid message type: {}".format(type_))

        if processed is not None:
            bypass_projects = get_config("replacements_bypass_projects", "[]")
            projects = json.loads(cast(str, bypass_projects))
            if processed.get_project_id() in projects:
                # For a persistent non rate limited logger
                logger.info(
                    f"Skipping replacement for project. Data {message}, Partition: {message.metadata.partition_index}, Offset: {message.metadata.offset}",
                )
                # For sentry tracking
                logger.error(
                    "Skipping replacement for project",
                    extra={"project_id": processed.get_project_id(), "data": message},
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


def _build_event_tombstone_replacement(
    message: Union[
        ReplacementMessage[EndDeleteGroupsMessageBody],
        ReplacementMessage[TombstoneEventsMessageBody],
    ],
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
        "project_id": message.data["project_id"],
    }
    final_query_args.update(query_args)

    return LegacyReplacement(
        count_query_template,
        insert_query_template,
        final_query_args,
        query_time_flags,
        replacement_type=message.action_type,
        replacement_message_metadata=message.metadata,
    )


def _build_group_replacement(
    message: Union[
        ReplacementMessage[EndMergeMessageBody],
        ReplacementMessage[ReplaceGroupMessageBody],
    ],
    project_id: int,
    where: str,
    query_args: Mapping[str, str],
    query_time_flags: LegacyQueryTimeFlags,
    all_columns: Sequence[FlattenedColumn],
) -> Optional[Replacement]:
    # HACK: We were sending duplicates of the `end_merge` message from Sentry,
    # this is only for performance of the backlog.
    txn = message.data.get("transaction_id")
    if txn:
        if txn in SEEN_MERGE_TXN_CACHE:
            logger.error(
                "Skipping duplicate group replacement", extra={"project_id": project_id}
            )
            return None
        else:
            SEEN_MERGE_TXN_CACHE.append(txn)

    all_column_names = [c.escaped for c in all_columns]
    select_columns = map(
        lambda i: i if i != "group_id" else str(message.data["new_group_id"]),
        all_column_names,
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
        count_query_template,
        insert_query_template,
        final_query_args,
        query_time_flags,
        replacement_type=message.action_type,
        replacement_message_metadata=message.metadata,
    )


def _build_event_set_filter(
    project_id: int,
    event_ids: Sequence[str],
    from_timestamp: Optional[str],
    to_timestamp: Optional[str],
) -> Tuple[List[str], List[str], MutableMapping[str, str]]:
    def get_timestamp_condition(msg_value: Optional[str], operator: str) -> str:
        if not msg_value:
            return ""

        timestamp = datetime.strptime(msg_value, settings.PAYLOAD_DATETIME_FORMAT)
        return (
            f"timestamp {operator} toDateTime('{timestamp.strftime(DATETIME_FORMAT)}')"
        )

    from_condition = get_timestamp_condition(from_timestamp, ">=")
    to_condition = get_timestamp_condition(to_timestamp, "<=")

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
    from_timestamp: Optional[str]
    to_timestamp: Optional[str]
    new_group_id: int
    replacement_type: ReplacementType
    replacement_message_metadata: ReplacementMessageMetadata
    all_columns: Sequence[FlattenedColumn]

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[ReplaceGroupMessageBody],
        context: ReplacementContext,
    ) -> Optional[ReplaceGroupReplacement]:
        event_ids = message.data["event_ids"]
        if not event_ids:
            return None

        return cls(
            event_ids=event_ids,
            project_id=message.data["project_id"],
            from_timestamp=message.data.get("from_timestamp"),
            to_timestamp=message.data.get("to_timestamp"),
            new_group_id=message.data["new_group_id"],
            replacement_type=message.action_type,
            replacement_message_metadata=message.metadata,
            all_columns=context.all_columns,
        )

    def get_project_id(self) -> int:
        return self.project_id

    def get_query_time_flags(self) -> Optional[QueryTimeFlags]:
        return None

    def get_replacement_type(self) -> ReplacementType:
        return self.replacement_type

    def get_message_metadata(self) -> ReplacementMessageMetadata:
        return self.replacement_message_metadata

    @cached_property
    def _where_clause(self) -> str:
        prewhere, where, query_args = _build_event_set_filter(
            project_id=self.project_id,
            event_ids=self.event_ids,
            from_timestamp=self.from_timestamp,
            to_timestamp=self.to_timestamp,
        )

        return (
            f"PREWHERE {' AND '.join(prewhere)} WHERE {' AND '.join(where)}"
            % query_args
        )

    def get_count_query(self, table_name: str) -> Optional[str]:
        return f"""\
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_insert_query(self, table_name: str) -> Optional[str]:
        all_column_names = [c.escaped for c in self.all_columns]
        select_columns = ", ".join(
            map(
                lambda i: i if i != "group_id" else str(self.new_group_id),
                all_column_names,
            )
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
    replacement_type: ReplacementType
    replacement_message_metadata: ReplacementMessageMetadata

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[EndDeleteGroupsMessageBody],
        context: ReplacementContext,
    ) -> Optional[DeleteGroupsReplacement]:

        group_ids = message.data["group_ids"]
        if not group_ids:
            return None

        assert all(isinstance(gid, int) for gid in group_ids)

        timestamp = datetime.strptime(
            message.data["datetime"], settings.PAYLOAD_DATETIME_FORMAT
        )

        return cls(
            required_columns=context.required_columns,
            project_id=message.data["project_id"],
            timestamp=timestamp,
            group_ids=group_ids,
            replacement_type=message.action_type,
            replacement_message_metadata=message.metadata,
        )

    def get_project_id(self) -> int:
        return self.project_id

    def get_query_time_flags(self) -> Optional[QueryTimeFlags]:
        return ExcludeGroups(group_ids=self.group_ids)

    def get_replacement_type(self) -> ReplacementType:
        return self.replacement_type

    def get_message_metadata(self) -> ReplacementMessageMetadata:
        return self.replacement_message_metadata

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

    def get_count_query(self, table_name: str) -> Optional[str]:
        return f"""\
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_insert_query(self, table_name: str) -> Optional[str]:
        required_columns = ", ".join(self.required_columns)
        select_columns = ", ".join(
            map(lambda i: i if i != "deleted" else "1", self.required_columns)
        )
        return f"""\
            INSERT INTO {table_name} ({required_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
            {self._where_clause}
        """


def process_tombstone_events(
    message: ReplacementMessage[TombstoneEventsMessageBody],
    required_columns: Sequence[str],
    state_name: ReplacerState,
) -> Optional[Replacement]:
    event_ids = message.data["event_ids"]
    if not event_ids:
        return None

    old_primary_hash = message.data.get("old_primary_hash")

    prewhere, where, query_args = _build_event_set_filter(
        project_id=message.data["project_id"],
        event_ids=event_ids,
        from_timestamp=message.data.get("from_timestamp"),
        to_timestamp=message.data.get("to_timestamp"),
    )

    if old_primary_hash:
        query_args["old_primary_hash"] = (
            ("'%s'" % (str(uuid.UUID(old_primary_hash)),))
            if old_primary_hash
            else "NULL"
        )

        prewhere.append("primary_hash = %(old_primary_hash)s")

    query_time_flags = (None, message.data["project_id"])

    full_where = f"PREWHERE {' AND '.join(prewhere)} WHERE {' AND '.join(where)}"

    return _build_event_tombstone_replacement(
        message, required_columns, full_where, query_args, query_time_flags
    )


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
    replacement_type: ReplacementType
    replacement_message_metadata: ReplacementMessageMetadata

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[ExcludeGroupsMessageBody],
        context: ReplacementContext,
    ) -> Optional[ExcludeGroupsReplacement]:
        if not message.data["group_ids"]:
            return None

        return cls(
            project_id=message.data["project_id"],
            group_ids=message.data["group_ids"],
            replacement_type=message.action_type,
            replacement_message_metadata=message.metadata,
        )

    def get_project_id(self) -> int:
        return self.project_id

    def get_query_time_flags(self) -> Optional[QueryTimeFlags]:
        return ExcludeGroups(group_ids=self.group_ids)

    def get_replacement_type(self) -> ReplacementType:
        return self.replacement_type

    def get_insert_query(self, table_name: str) -> Optional[str]:
        return None

    def get_count_query(self, table_name: str) -> Optional[str]:
        return None

    def get_message_metadata(self) -> ReplacementMessageMetadata:
        return self.replacement_message_metadata


SEEN_MERGE_TXN_CACHE: Deque[str] = deque(maxlen=100)


def process_merge(
    message: ReplacementMessage[EndMergeMessageBody],
    all_columns: Sequence[FlattenedColumn],
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

    previous_group_ids = message.data["previous_group_ids"]
    if not previous_group_ids:
        return None

    assert all(isinstance(gid, int) for gid in previous_group_ids)

    timestamp = datetime.strptime(
        message.data["datetime"], settings.PAYLOAD_DATETIME_FORMAT
    )

    query_args = {
        "previous_group_ids": ", ".join(str(gid) for gid in previous_group_ids),
        "timestamp": timestamp.strftime(DATETIME_FORMAT),
    }

    project_id: int = message.data["project_id"]
    query_time_flags = (EXCLUDE_GROUPS, project_id, previous_group_ids)

    return _build_group_replacement(
        message,
        project_id,
        where,
        query_args,
        query_time_flags,
        all_columns,
    )


@dataclass(frozen=True)
class UnmergeGroupsReplacement(Replacement):
    state_name: ReplacerState
    timestamp: datetime
    hashes: Sequence[str]
    all_columns: Sequence[FlattenedColumn]
    project_id: int
    previous_group_id: int
    new_group_id: int
    replacement_type: ReplacementType
    replacement_message_metadata: ReplacementMessageMetadata

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[EndUnmergeMessageBody],
        context: ReplacementContext,
    ) -> Optional["Replacement"]:
        hashes = message.data["hashes"]
        if not hashes:
            return None

        assert all(isinstance(h, str) for h in hashes)

        timestamp = datetime.strptime(
            message.data["datetime"], settings.PAYLOAD_DATETIME_FORMAT
        )

        return UnmergeGroupsReplacement(
            state_name=context.state_name,
            timestamp=timestamp,
            hashes=hashes,
            project_id=message.data["project_id"],
            previous_group_id=message.data["previous_group_id"],
            new_group_id=message.data["new_group_id"],
            all_columns=context.all_columns,
            replacement_type=message.action_type,
            replacement_message_metadata=message.metadata,
        )

    def get_project_id(self) -> int:
        return self.project_id

    def get_query_time_flags(self) -> Optional[QueryTimeFlags]:
        return NeedsFinal()

    def get_replacement_type(self) -> ReplacementType:
        return self.replacement_type

    def get_message_metadata(self) -> ReplacementMessageMetadata:
        return self.replacement_message_metadata

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
            PREWHERE primary_hash IN ({hashes})
            WHERE group_id = {self.previous_group_id}
            AND project_id = {self.project_id}
            AND received <= CAST('{timestamp}' AS DateTime)
            AND NOT deleted
        """

    def get_count_query(self, table_name: str) -> Optional[str]:
        return f"""\
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_insert_query(self, table_name: str) -> Optional[str]:
        all_column_names = [c.escaped for c in self.all_columns]
        select_columns = ", ".join(
            map(
                lambda i: i if i != "group_id" else str(self.new_group_id),
                all_column_names,
            )
        )

        all_columns = ", ".join(all_column_names)

        return f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
            {self._where_clause}
        """


def _convert_hash(
    hash: str, state_name: ReplacerState, convert_types: bool = False
) -> str:
    if state_name == ReplacerState.ERRORS:
        if convert_types:
            return "toUUID('%s')" % str(uuid.UUID(_hashify(hash)))
        else:
            return "'%s'" % str(uuid.UUID(_hashify(hash)))
    else:
        if convert_types:
            return "toFixedString('%s', 32)" % _hashify(hash)
        else:
            return "'%s'" % _hashify(hash)


@dataclass
class UnmergeHierarchicalReplacement(Replacement):
    project_id: int
    timestamp: datetime
    primary_hash: str
    hierarchical_hash: str
    previous_group_id: int
    new_group_id: int

    all_columns: Sequence[FlattenedColumn]
    state_name: ReplacerState

    replacement_type: ReplacementType
    replacement_message_metadata: ReplacementMessageMetadata

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[EndUnmergeHierarchicalMessageBody],
        context: ReplacementContext,
    ) -> Optional[UnmergeHierarchicalReplacement]:
        try:
            timestamp = datetime.strptime(
                message.data["datetime"], settings.PAYLOAD_DATETIME_FORMAT
            )

            primary_hash = message.data["primary_hash"]
            assert isinstance(primary_hash, str)

            hierarchical_hash = message.data["hierarchical_hash"]
            assert isinstance(hierarchical_hash, str)

            uuid.UUID(primary_hash)
            uuid.UUID(hierarchical_hash)
        except Exception as exc:
            # TODO(markus): We're sacrificing consistency over uptime as long as
            # this is in development. At some point this piece of code should be
            # stable enough to remove this.
            logger.error("process_unmerge_hierarchical.failed", exc_info=exc)
            return None

        return cls(
            project_id=message.data["project_id"],
            timestamp=timestamp,
            primary_hash=primary_hash,
            hierarchical_hash=hierarchical_hash,
            all_columns=context.all_columns,
            state_name=context.state_name,
            replacement_type=message.action_type,
            replacement_message_metadata=message.metadata,
            previous_group_id=message.data["previous_group_id"],
            new_group_id=message.data["new_group_id"],
        )

    @cached_property
    def _where_clause(self) -> str:
        primary_hash = _convert_hash(self.primary_hash, self.state_name)
        hierarchical_hash = _convert_hash(
            self.hierarchical_hash, self.state_name, convert_types=True
        )
        timestamp = self.timestamp.strftime(DATETIME_FORMAT)

        return f"""\
            PREWHERE primary_hash = {primary_hash}
            WHERE group_id = {self.previous_group_id}
            AND has(hierarchical_hashes, {hierarchical_hash})
            AND project_id = {self.project_id}
            AND received <= CAST('{timestamp}' AS DateTime)
            AND NOT deleted
        """

    def get_count_query(self, table_name: str) -> Optional[str]:
        return f"""
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_insert_query(self, table_name: str) -> Optional[str]:
        all_column_names = [c.escaped for c in self.all_columns]
        all_columns = ", ".join(all_column_names)
        select_columns = ", ".join(
            map(
                lambda i: i if i != "group_id" else str(self.new_group_id),
                all_column_names,
            )
        )

        return f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_query_time_flags(self) -> Optional[QueryTimeFlags]:
        return None

    def get_project_id(self) -> int:
        return self.project_id

    def get_replacement_type(self) -> ReplacementType:
        return self.replacement_type

    def get_message_metadata(self) -> ReplacementMessageMetadata:
        return self.replacement_message_metadata


@dataclass
class DeleteTagReplacement(Replacement):
    project_id: int
    tag: str
    timestamp: datetime
    tag_column_name: str
    is_promoted: bool
    all_columns: Sequence[FlattenedColumn]
    schema: WritableTableSchema
    replacement_type: ReplacementType
    replacement_message_metadata: ReplacementMessageMetadata

    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[EndDeleteTagMessageBody],
        context: ReplacementContext,
    ) -> Optional[DeleteTagReplacement]:
        tag = message.data["tag"]
        if not tag:
            return None

        assert isinstance(tag, str)
        timestamp = datetime.strptime(
            message.data["datetime"], settings.PAYLOAD_DATETIME_FORMAT
        )
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
            replacement_type=message.action_type,
            replacement_message_metadata=message.metadata,
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
                column_type = (
                    self.schema.get_data_source()
                    .get_columns()
                    .get(self.tag_column_name)
                )
                assert column_type is not None
                is_nullable = column_type.type.has_modifier(Nullable)
                if is_nullable:
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

    def get_insert_query(self, table_name: str) -> Optional[str]:
        all_columns = ", ".join(col.escaped for col in self.all_columns)
        select_columns = ", ".join(self._select_columns)

        return f"""\
            INSERT INTO {table_name} ({all_columns})
            SELECT {select_columns}
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_count_query(self, table_name: str) -> Optional[str]:
        return f"""\
            SELECT count()
            FROM {table_name} FINAL
            {self._where_clause}
        """

    def get_query_time_flags(self) -> QueryTimeFlags:
        return NeedsFinal()

    def get_project_id(self) -> int:
        return self.project_id

    def get_replacement_type(self) -> ReplacementType:
        return self.replacement_type

    def get_message_metadata(self) -> ReplacementMessageMetadata:
        return self.replacement_message_metadata
