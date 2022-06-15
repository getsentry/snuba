from __future__ import annotations

import importlib
from datetime import datetime, timedelta
from typing import Any, Mapping, MutableMapping, Sequence

import pytz
import simplejson as json
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload

from snuba import replacer, settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets import errors_replacer
from snuba.datasets.errors_replacer import ProjectsQueryFlags
from snuba.datasets.events_processor_base import ReplacementType
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.optimize import run_optimize
from snuba.redis import redis_client
from snuba.replacers.replacer_processor import ReplacerState
from snuba.settings import PAYLOAD_DATETIME_FORMAT
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from tests.fixtures import get_raw_event
from tests.helpers import write_unprocessed_events

CONSUMER_GROUP = "consumer_group"


class TestReplacer:
    def setup_method(self) -> None:
        from snuba.web.views import application

        assert application.testing is True

        self.app = application.test_client()
        self.storage = get_writable_storage(StorageKey.ERRORS)

        self.replacer = replacer.ReplacerWorker(
            self.storage,
            CONSUMER_GROUP,
            DummyMetricsBackend(strict=True),
        )

        self.project_id = 1
        self.event = get_raw_event()

    def teardown_method(self) -> None:
        importlib.reload(settings)

    def _wrap(self, msg: str) -> Message[KafkaPayload]:
        return Message(
            Partition(Topic("replacements"), 0),
            0,
            KafkaPayload(None, json.dumps(msg).encode("utf-8"), []),
            datetime.now(),
        )

    def _clear_redis_and_force_merge(self) -> None:
        redis_client.flushdb()
        cluster = self.storage.get_cluster()
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
        run_optimize(clickhouse, self.storage, cluster.get_database())

    def _issue_count(self, project_id: int) -> Sequence[Mapping[str, Any]]:
        clickhouse = self.storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.QUERY
        )

        data = clickhouse.execute(
            f"""
            SELECT group_id, count()
            FROM errors_local
            FINAL
            WHERE deleted = 0
            AND project_id = {project_id}
            GROUP BY group_id
            """
        ).results

        return [{"group_id": row[0], "count": row[1]} for row in data]

    def test_unmerge_hierarchical_insert(self) -> None:
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.event["primary_hash"] = "b" * 32
        self.event["data"]["hierarchical_hashes"] = ["a" * 32]
        write_unprocessed_events(self.storage, [self.event])

        assert self._issue_count(self.project_id) == [{"count": 1, "group_id": 1}]

        timestamp = datetime.now(tz=pytz.utc)

        project_id = self.project_id

        message: Message[KafkaPayload] = Message(
            Partition(Topic("replacements"), 1),
            42,
            KafkaPayload(
                None,
                json.dumps(
                    (
                        2,
                        ReplacementType.END_UNMERGE_HIERARCHICAL,
                        {
                            "project_id": project_id,
                            "previous_group_id": 1,
                            "new_group_id": 2,
                            "hierarchical_hash": "a" * 32,
                            "primary_hash": "b" * 32,
                            "datetime": timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                        },
                    )
                ).encode("utf-8"),
                [],
            ),
            datetime.now(),
        )

        processed = self.replacer.process_message(message)
        assert processed is not None
        self.replacer.flush_batch([processed])

        assert self._issue_count(self.project_id) == [{"count": 1, "group_id": 2}]

    def test_delete_tag_promoted_insert(self) -> None:
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.event["data"]["tags"].append(["browser.name", "foo"])
        self.event["data"]["tags"].append(["notbrowser", "foo"])
        write_unprocessed_events(self.storage, [self.event])

        project_id = self.project_id

        def _issue_count(total: bool = False) -> Sequence[Mapping[str, Any]]:
            clickhouse = self.storage.get_cluster().get_query_connection(
                ClickhouseClientSettings.QUERY
            )

            total_cond = (
                "AND has(_tags_hash_map, cityHash64('browser.name=foo'))"
                if not total
                else ""
            )

            data = clickhouse.execute(
                f"""
                SELECT group_id, count()
                FROM errors_local
                FINAL
                WHERE deleted = 0
                AND project_id = {project_id}
                {total_cond}
                GROUP BY group_id
                """
            ).results

            return [{"group_id": row[0], "count": row[1]} for row in data]

        assert _issue_count() == [{"count": 1, "group_id": 1}]
        assert _issue_count(total=True) == [{"count": 1, "group_id": 1}]

        timestamp = datetime.now(tz=pytz.utc)

        message: Message[KafkaPayload] = Message(
            Partition(Topic("replacements"), 1),
            42,
            KafkaPayload(
                None,
                json.dumps(
                    (
                        2,
                        ReplacementType.END_DELETE_TAG,
                        {
                            "project_id": project_id,
                            "tag": "browser.name",
                            "datetime": timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                        },
                    )
                ).encode("utf-8"),
                [],
            ),
            datetime.now(),
        )

        processed = self.replacer.process_message(message)
        assert processed is not None
        self.replacer.flush_batch([processed])

        assert _issue_count() == []
        assert _issue_count(total=True) == [{"count": 1, "group_id": 1}]

    def test_latest_replacement_time_by_projects(self) -> None:
        project_ids = [1, 2, 3]
        p = redis_client.pipeline()

        exclude_groups_keys = [
            errors_replacer.ProjectsQueryFlags._build_project_exclude_groups_key_and_type_key(
                project_id, ReplacerState.ERRORS
            )
            for project_id in project_ids
        ]

        project_needs_final_keys = [
            errors_replacer.ProjectsQueryFlags._build_project_needs_final_key_and_type_key(
                project_id, ReplacerState.ERRORS
            )
            for project_id in project_ids
        ]

        now = datetime.now()

        # No replacements or needs final
        flags = ProjectsQueryFlags.load_from_redis(project_ids, ReplacerState.ERRORS)
        assert flags.latest_replacement_time is None

        # All projects need final
        time_offset = 0
        for project_needs_final_key, _ in project_needs_final_keys:
            p.set(project_needs_final_key, now.timestamp() + time_offset)
            time_offset += 10
        p.execute()
        flags = ProjectsQueryFlags.load_from_redis(project_ids, ReplacerState.ERRORS)
        expected_time = now + timedelta(seconds=20)
        assert (
            flags.latest_replacement_time is not None
            and abs((flags.latest_replacement_time - expected_time).total_seconds()) < 1
        )
        redis_client.flushdb()

        # Some projects need final
        time_offset = 0
        for project_needs_final_key, _ in project_needs_final_keys[1:]:
            p.set(project_needs_final_key, now.timestamp() + time_offset)
            time_offset += 10
        p.execute()
        flags = ProjectsQueryFlags.load_from_redis(project_ids, ReplacerState.ERRORS)
        expected_time = now + timedelta(seconds=10)
        assert (
            flags.latest_replacement_time is not None
            and abs((flags.latest_replacement_time - expected_time).total_seconds()) < 1
        )
        redis_client.flushdb()

        # One exclude group per project
        group_id_data_asc: MutableMapping[str, float] = {"1": now.timestamp()}
        for exclude_groups_key, _ in exclude_groups_keys:
            group_id_data_asc["1"] += 10
            to_insert: Mapping[str | bytes, bytes | int | float | str] = {
                "1": group_id_data_asc["1"],
            }  # typing error fix
            p.zadd(exclude_groups_key, to_insert)
        p.execute()
        expected_time = now + timedelta(seconds=30)
        flags = ProjectsQueryFlags.load_from_redis(project_ids, ReplacerState.ERRORS)
        assert (
            flags.latest_replacement_time is not None
            and abs((flags.latest_replacement_time - expected_time).total_seconds()) < 1
        )
        redis_client.flushdb()

        # Multiple exclude groups per project
        group_id_data_multiple: MutableMapping[str, float] = {
            "1": (now + timedelta(seconds=10)).timestamp(),
            "2": now.timestamp(),
        }
        for exclude_groups_key, _ in exclude_groups_keys:
            group_id_data_multiple["1"] -= 10
            group_id_data_multiple["2"] -= 10
            to_insert = {
                "1": group_id_data_multiple["1"],
                "2": group_id_data_multiple["2"],
            }  # typing error fix
            p.zadd(exclude_groups_key, to_insert)
        p.execute()
        expected_time = now
        flags = ProjectsQueryFlags.load_from_redis(project_ids, ReplacerState.ERRORS)
        assert (
            flags.latest_replacement_time is not None
            and abs((flags.latest_replacement_time - expected_time).total_seconds()) < 1
        )
        redis_client.flushdb()

    def test_query_time_flags_project(self) -> None:
        """
        Tests errors_replacer.set_project_needs_final()

        ReplacementType's are arbitrary, just need to show up in
        getter appropriately once set.
        """
        redis_client.flushdb()
        project_ids = [1, 2, 3]
        assert ProjectsQueryFlags.load_from_redis(
            project_ids, ReplacerState.ERRORS
        ) == ProjectsQueryFlags(False, set(), set(), None)

        errors_replacer.set_project_needs_final(
            100, ReplacerState.ERRORS, ReplacementType.EXCLUDE_GROUPS
        )
        assert ProjectsQueryFlags.load_from_redis(
            project_ids, ReplacerState.ERRORS
        ) == ProjectsQueryFlags(False, set(), set(), None)

        errors_replacer.set_project_needs_final(
            1, ReplacerState.ERRORS, ReplacementType.EXCLUDE_GROUPS
        )
        flags = ProjectsQueryFlags.load_from_redis(project_ids, ReplacerState.ERRORS)
        assert (
            flags.needs_final,
            flags.group_ids_to_exclude,
            flags.replacement_types,
        ) == (
            True,
            set(),
            {ReplacementType.EXCLUDE_GROUPS},
        )

        errors_replacer.set_project_needs_final(
            2, ReplacerState.ERRORS, ReplacementType.EXCLUDE_GROUPS
        )
        flags = ProjectsQueryFlags.load_from_redis(project_ids, ReplacerState.ERRORS)
        assert (
            flags.needs_final,
            flags.group_ids_to_exclude,
            flags.replacement_types,
        ) == (
            True,
            set(),
            {ReplacementType.EXCLUDE_GROUPS},
        )

    def test_query_time_flags_groups(self) -> None:
        """
        Tests errors_replacer.set_project_exclude_groups()

        ReplacementType's are arbitrary, just need to show up in
        getter appropriately once set.
        """
        redis_client.flushdb()
        project_ids = [4, 5, 6]
        errors_replacer.set_project_exclude_groups(
            4, [1, 2], ReplacerState.ERRORS, ReplacementType.EXCLUDE_GROUPS
        )
        errors_replacer.set_project_exclude_groups(
            5, [3, 4], ReplacerState.ERRORS, ReplacementType.START_MERGE
        )
        flags = ProjectsQueryFlags.load_from_redis(project_ids, ReplacerState.ERRORS)
        assert (
            flags.needs_final,
            flags.group_ids_to_exclude,
            flags.replacement_types,
        ) == (
            False,
            {1, 2, 3, 4},
            {ReplacementType.EXCLUDE_GROUPS, ReplacementType.START_MERGE},
        )

        errors_replacer.set_project_exclude_groups(
            4, [1, 2], ReplacerState.ERRORS, ReplacementType.EXCLUDE_GROUPS
        )
        errors_replacer.set_project_exclude_groups(
            5, [3, 4], ReplacerState.ERRORS, ReplacementType.EXCLUDE_GROUPS
        )
        errors_replacer.set_project_exclude_groups(
            6, [5, 6], ReplacerState.ERRORS, ReplacementType.START_UNMERGE
        )

        flags = ProjectsQueryFlags.load_from_redis(project_ids, ReplacerState.ERRORS)
        assert (
            flags.needs_final,
            flags.group_ids_to_exclude,
            flags.replacement_types,
        ) == (
            False,
            {1, 2, 3, 4, 5, 6},
            {
                ReplacementType.EXCLUDE_GROUPS,
                # start_merge should show up from previous setter on project id 2
                ReplacementType.START_MERGE,
                ReplacementType.START_UNMERGE,
            },
        )
        flags = ProjectsQueryFlags.load_from_redis([4, 5], ReplacerState.ERRORS)
        assert (
            flags.needs_final,
            flags.group_ids_to_exclude,
            flags.replacement_types,
        ) == (
            False,
            {1, 2, 3, 4},
            {ReplacementType.EXCLUDE_GROUPS, ReplacementType.START_MERGE},
        )
        flags = ProjectsQueryFlags.load_from_redis([4], ReplacerState.ERRORS)
        assert (
            flags.needs_final,
            flags.group_ids_to_exclude,
            flags.replacement_types,
        ) == (
            False,
            {1, 2},
            {ReplacementType.EXCLUDE_GROUPS},
        )

    def test_query_time_flags_project_and_groups(self) -> None:
        """
        Tests errors_replacer.set_project_needs_final() and
        errors_replacer.set_project_exclude_groups() work together as expected.

        ReplacementType's are arbitrary, just need to show up in
        getter appropriately once set.
        """
        redis_client.flushdb()
        project_ids = [7, 8, 9]

        errors_replacer.set_project_needs_final(
            7, ReplacerState.ERRORS, ReplacementType.EXCLUDE_GROUPS
        )
        errors_replacer.set_project_exclude_groups(
            7, [1, 2], ReplacerState.ERRORS, ReplacementType.START_MERGE
        )
        flags = ProjectsQueryFlags.load_from_redis(project_ids, ReplacerState.ERRORS)
        assert (
            flags.needs_final,
            flags.group_ids_to_exclude,
            flags.replacement_types,
        ) == (
            True,
            {1, 2},
            # exclude_groups from project setter, start_merge from group setter
            {ReplacementType.EXCLUDE_GROUPS, ReplacementType.START_MERGE},
        )
