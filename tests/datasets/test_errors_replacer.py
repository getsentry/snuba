import importlib
import re
import uuid
from datetime import datetime
from functools import partial
from typing import Any, Tuple

import pytest
import pytz
import simplejson as json

from snuba import replacer, settings
from snuba.clickhouse import DATETIME_FORMAT
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets import errors_replacer
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.optimize import run_optimize
from snuba.redis import redis_client
from snuba.settings import PAYLOAD_DATETIME_FORMAT
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams import Message, Partition, Topic
from snuba.utils.streams.backends.kafka import KafkaPayload
from tests.fixtures import get_raw_event
from tests.helpers import write_unprocessed_events


class TestReplacer:
    def setup_method(self):
        from snuba.web.views import application

        assert application.testing is True

        self.app = application.test_client()
        self.app.post = partial(self.app.post, headers={"referer": "test"})

        self.storage = get_writable_storage(StorageKey.ERRORS)
        self.replacer = replacer.ReplacerWorker(
            self.storage, DummyMetricsBackend(strict=True)
        )

        self.project_id = 1
        self.event = get_raw_event()
        settings.ERRORS_ROLLOUT_ALL = True
        settings.ERRORS_ROLLOUT_WRITABLE_STORAGE = True

    def teardown_method(self):
        importlib.reload(settings)

    def _wrap(self, msg: Tuple[Any, ...]) -> Message[KafkaPayload]:
        return Message(
            Partition(Topic("replacements"), 0),
            0,
            KafkaPayload(None, json.dumps(msg).encode("utf-8"), []),
            datetime.now(),
        )

    def _clear_redis_and_force_merge(self):
        redis_client.flushdb()
        cluster = self.storage.get_cluster()
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
        run_optimize(clickhouse, self.storage, cluster.get_database())

    def _issue_count(self, project_id, group_id=None):
        args = {
            "project": [project_id],
            "aggregations": [["count()", "", "count"]],
            "groupby": ["group_id"],
        }

        if group_id:
            args.setdefault("conditions", []).append(("group_id", "=", group_id))

        return json.loads(self.app.post("/events/query", data=json.dumps(args)).data)[
            "data"
        ]

    def _get_group_id(self, project_id: int, event_id: str):
        args = {
            "project": [project_id],
            "selected_columns": ["group_id"],
            "conditions": [
                ["event_id", "=", str(uuid.UUID(event_id)).replace("-", "")]
            ],
        }

        data = json.loads(self.app.post("/events/query", data=json.dumps(args)).data)[
            "data"
        ]
        if not data:
            return None

        return data[0]["group_id"]

    def test_delete_groups_process(self) -> None:
        timestamp = datetime.now(tz=pytz.utc)
        message = (
            2,
            "end_delete_groups",
            {
                "project_id": self.project_id,
                "group_ids": [1, 2, 3],
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        replacement = self.replacer.process_message(self._wrap(message))

        assert (
            re.sub("[\n ]+", " ", replacement.count_query_template).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE group_id IN (%(group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(table_name)s (%(required_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE group_id IN (%(group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert replacement.query_args == {
            "group_ids": "1, 2, 3",
            "project_id": self.project_id,
            "required_columns": "event_id, primary_hash, project_id, group_id, timestamp, deleted, retention_days",
            "select_columns": "event_id, primary_hash, project_id, group_id, timestamp, 1, retention_days",
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
        }
        assert replacement.query_time_flags == (
            errors_replacer.EXCLUDE_GROUPS,
            self.project_id,
            [1, 2, 3],
        )

    @pytest.mark.parametrize("for_primary_hash_change", [True, False, None])
    def test_tombstone_events_process(self, for_primary_hash_change) -> None:
        timestamp = datetime.now(tz=pytz.utc)
        message_kwargs = {
            "project_id": self.project_id,
            "event_ids": ["00e24a150d7f4ee4b142b61b4d893b6d"],
            "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }

        if for_primary_hash_change is not None:
            message_kwargs["for_primary_hash_change"] = for_primary_hash_change

        message = (2, "tombstone_events", message_kwargs)

        replacement = self.replacer.process_message(self._wrap(message))

        assert (
            re.sub("[\n ]+", " ", replacement.count_query_template).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE replaceAll(toString(event_id), '-', '') IN (%(event_ids)s) WHERE project_id = %(project_id)s AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(table_name)s (%(required_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE replaceAll(toString(event_id), '-', '') IN (%(event_ids)s) WHERE project_id = %(project_id)s AND NOT deleted"
        )
        assert replacement.query_args == {
            "event_ids": "'00e24a150d7f4ee4b142b61b4d893b6d'",
            "project_id": self.project_id,
            "required_columns": "event_id, primary_hash, project_id, group_id, timestamp, deleted, retention_days",
            "select_columns": "event_id, primary_hash, project_id, group_id, timestamp, 1, retention_days",
        }
        assert replacement.query_time_flags == (None, self.project_id,)

    def test_replace_group_process(self) -> None:
        timestamp = datetime.now(tz=pytz.utc)
        message = (
            2,
            "replace_group",
            {
                "project_id": self.project_id,
                "event_ids": ["00e24a150d7f4ee4b142b61b4d893b6d"],
                "new_group_id": 2,
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        replacement = self.replacer.process_message(self._wrap(message))

        assert (
            re.sub("[\n ]+", " ", replacement.count_query_template).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE replaceAll(toString(event_id), '-', '') IN (%(event_ids)s) WHERE project_id = %(project_id)s AND NOT deleted"
        )

        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE replaceAll(toString(event_id), '-', '') IN (%(event_ids)s) WHERE project_id = %(project_id)s AND NOT deleted"
        )
        assert replacement.query_args == {
            "event_ids": "'00e24a150d7f4ee4b142b61b4d893b6d'",
            "project_id": self.project_id,
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, 2, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
        }
        assert replacement.query_time_flags == (None, self.project_id,)

    def test_merge_process(self) -> None:
        timestamp = datetime.now(tz=pytz.utc)
        message = (
            2,
            "end_merge",
            {
                "project_id": self.project_id,
                "new_group_id": 2,
                "previous_group_ids": [1, 2],
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        replacement = self.replacer.process_message(self._wrap(message))

        assert (
            re.sub("[\n ]+", " ", replacement.count_query_template).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE group_id IN (%(previous_group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE group_id IN (%(previous_group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert replacement.query_args == {
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, 2, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "previous_group_ids": ", ".join(str(gid) for gid in [1, 2]),
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
        }
        assert replacement.query_time_flags == (
            errors_replacer.EXCLUDE_GROUPS,
            self.project_id,
            [1, 2],
        )

    def test_unmerge_process(self) -> None:
        timestamp = datetime.now(tz=pytz.utc)
        message = (
            2,
            "end_unmerge",
            {
                "project_id": self.project_id,
                "previous_group_id": 1,
                "new_group_id": 2,
                "hashes": ["a" * 32, "b" * 32],
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        replacement = self.replacer.process_message(self._wrap(message))

        assert (
            re.sub("[\n ]+", " ", replacement.count_query_template).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE group_id = %(previous_group_id)s WHERE project_id = %(project_id)s AND primary_hash IN (%(hashes)s) AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE group_id = %(previous_group_id)s WHERE project_id = %(project_id)s AND primary_hash IN (%(hashes)s) AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert replacement.query_args == {
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, 2, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "hashes": "'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'",
            "previous_group_id": 1,
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
        }

        assert replacement.query_time_flags == (
            errors_replacer.NEEDS_FINAL,
            self.project_id,
        )

    def test_delete_promoted_tag_process(self) -> None:
        timestamp = datetime.now(tz=pytz.utc)
        message = (
            2,
            "end_delete_tag",
            {
                "project_id": self.project_id,
                "tag": "sentry:user",
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        replacement = self.replacer.process_message(self._wrap(message))

        assert (
            re.sub("[\n ]+", " ", replacement.count_query_template).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE has(`tags.key`, %(tag_str)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE has(`tags.key`, %(tag_str)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert replacement.query_args == {
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, '', user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, 'sentry:user')), `tags.key`), arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, 'sentry:user'), arrayEnumerate(`tags.value`))), contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "tag_column": "user",
            "tag_str": "'sentry:user'",
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
        }
        assert replacement.query_time_flags == (
            errors_replacer.NEEDS_FINAL,
            self.project_id,
        )

    def test_delete_unpromoted_tag_process(self) -> None:
        timestamp = datetime.now(tz=pytz.utc)
        message = (
            2,
            "end_delete_tag",
            {
                "project_id": self.project_id,
                "tag": "foo:bar",
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        replacement = self.replacer.process_message(self._wrap(message))

        assert (
            re.sub("[\n ]+", " ", replacement.count_query_template).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE has(`tags.key`, %(tag_str)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE has(`tags.key`, %(tag_str)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )

        assert replacement.query_args == {
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, 'foo:bar')), `tags.key`), arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, 'foo:bar'), arrayEnumerate(`tags.value`))), contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "tag_column": "`foo:bar`",
            "tag_str": "'foo:bar'",
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
        }
        assert replacement.query_time_flags == (
            errors_replacer.NEEDS_FINAL,
            self.project_id,
        )

    def test_delete_groups_insert(self) -> None:
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
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
                        "end_delete_groups",
                        {
                            "project_id": project_id,
                            "group_ids": [1],
                            "datetime": timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                        },
                    )
                ).encode("utf-8"),
                [],
            ),
            datetime.now(),
        )

        processed = self.replacer.process_message(message)
        self.replacer.flush_batch([processed])

        assert self._issue_count(self.project_id) == []

        # Count is still zero after Redis flushed and parts merged
        self._clear_redis_and_force_merge()
        assert self._issue_count(self.project_id) == []

    def test_reprocessing_flow_insert(self) -> None:
        # We have a group that contains two events, 1 and 2.
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.event["event_id"] = event_id = "00e24a150d7f4ee4b142b61b4d893b6d"
        write_unprocessed_events(self.storage, [self.event])
        self.event["event_id"] = event_id2 = "00e24a150d7f4ee4b142b61b4d893b6e"
        write_unprocessed_events(self.storage, [self.event])

        assert self._issue_count(self.project_id) == [{"count": 2, "group_id": 1}]

        project_id = self.project_id

        message: Message[KafkaPayload] = Message(
            Partition(Topic("replacements"), 1),
            42,
            KafkaPayload(
                None,
                json.dumps(
                    (
                        2,
                        "tombstone_events",
                        {"project_id": project_id, "event_ids": [event_id]},
                    )
                ).encode("utf-8"),
                [],
            ),
            datetime.now(),
        )

        # The user chooses to reprocess a subset of the group and throw away
        # the other events. Event 1 gets manually tombstoned by Sentry while
        # Event 2 prevails.
        processed = self.replacer.process_message(message)
        self.replacer.flush_batch([processed])

        # At this point the count doesn't make any sense but we don't care.
        assert self._issue_count(self.project_id) == [{"count": 2, "group_id": 1}]

        # The reprocessed event is inserted with a guaranteed-new group ID but
        # the *same* event ID (this is why we need to skip tombstoning this
        # event ID)
        self.event["group_id"] = 2
        write_unprocessed_events(self.storage, [self.event])

        message: Message[KafkaPayload] = Message(
            Partition(Topic("replacements"), 1),
            42,
            KafkaPayload(
                None,
                json.dumps(
                    (2, "exclude_groups", {"project_id": project_id, "group_ids": [1]},)
                ).encode("utf-8"),
                [],
            ),
            datetime.now(),
        )

        # Group 1 is excluded from queries. At this point we have almost a
        # regular group deletion, except only a subset of events have been
        # tombstoned (the ones that will *not* be reprocessed).
        processed = self.replacer.process_message(message)
        self.replacer.flush_batch([processed])

        # Group 2 should contain the one event that the user chose to
        # reprocess, and Group 1 should be gone. (Note: In the product Group 2
        # looks identical to Group 1, including short ID).
        assert self._issue_count(self.project_id) == [{"count": 1, "group_id": 2}]
        assert self._get_group_id(project_id, event_id2) == 2
        assert not self._get_group_id(project_id, event_id)

    def test_merge_insert(self) -> None:
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
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
                        "end_merge",
                        {
                            "project_id": project_id,
                            "new_group_id": 2,
                            "previous_group_ids": [1],
                            "datetime": timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                        },
                    )
                ).encode("utf-8"),
                [],
            ),
            datetime.now(),
        )

        processed = self.replacer.process_message(message)
        self.replacer.flush_batch([processed])

        assert self._issue_count(1) == [{"count": 1, "group_id": 2}]

    def test_unmerge_insert(self) -> None:
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.event["primary_hash"] = "a" * 32
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
                        "end_unmerge",
                        {
                            "project_id": project_id,
                            "previous_group_id": 1,
                            "new_group_id": 2,
                            "hashes": ["a" * 32],
                            "datetime": timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                        },
                    )
                ).encode("utf-8"),
                [],
            ),
            datetime.now(),
        )

        processed = self.replacer.process_message(message)
        self.replacer.flush_batch([processed])

        assert self._issue_count(self.project_id) == [{"count": 1, "group_id": 2}]
