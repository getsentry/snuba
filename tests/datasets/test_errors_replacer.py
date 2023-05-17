import importlib
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, Tuple, Union

import pytest
import simplejson as json
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba import replacer, settings
from snuba.clickhouse import DATETIME_FORMAT
from snuba.clickhouse.optimize.optimize import run_optimize
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import ReplacementType
from snuba.redis import RedisClientKey, get_redis_client
from snuba.replacers import errors_replacer
from snuba.settings import PAYLOAD_DATETIME_FORMAT
from snuba.state import delete_config, set_config
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from tests.fixtures import get_raw_event
from tests.helpers import write_unprocessed_events

redis_client = get_redis_client(RedisClientKey.REPLACEMENTS_STORE)

CONSUMER_GROUP = "consumer_group"


class BaseTest:
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "events"

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

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

        # Total query time range is 24h before to 24h after now to account
        # for local machine time zones
        self.from_time = datetime.now().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(days=1)

        self.to_time = self.from_time + timedelta(days=2)

        self.project_id = 1
        self.event = get_raw_event()

    def _wrap(self, msg: Tuple[Any, ...]) -> Message[KafkaPayload]:
        return Message(
            BrokerValue(
                KafkaPayload(None, json.dumps(msg).encode("utf-8"), []),
                Partition(Topic("replacements"), 0),
                0,
                datetime.now(),
            )
        )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestReplacer(BaseTest):
    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[..., Any]) -> None:
        self.post = _build_snql_post_methods

    def teardown_method(self) -> None:
        importlib.reload(settings)

    def _clear_redis_and_force_merge(self) -> None:
        redis_client.flushdb()
        get_redis_client(RedisClientKey.CACHE).flushdb()
        cluster = self.storage.get_cluster()
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
        run_optimize(clickhouse, self.storage, cluster.get_database())

    def _issue_count(self, project_id: int, group_id: Optional[int] = None) -> Any:
        args = {
            "project": [project_id],
            "selected_columns": [],
            "aggregations": [["count()", "", "count"]],
            "groupby": ["group_id"],
            "from_date": self.from_time.isoformat(),
            "to_date": self.to_time.isoformat(),
            "tenant_ids": {"referrer": "r", "organization_id": 1234},
        }

        if group_id:
            args.setdefault("conditions", list()).append(("group_id", "=", group_id))

        return json.loads(self.post(json.dumps(args)).data)["data"]

    def _get_group_id(self, project_id: int, event_id: str) -> Optional[int]:
        args = {
            "project": [project_id],
            "selected_columns": ["group_id"],
            "conditions": [
                ["event_id", "=", str(uuid.UUID(event_id)).replace("-", "")]
            ],
            "from_date": self.from_time.isoformat(),
            "to_date": self.to_time.isoformat(),
            "tenant_ids": {"referrer": "r", "organization_id": 1234},
        }

        data = json.loads(self.post(json.dumps(args)).data)["data"]
        if not data:
            return None

        return int(data[0]["group_id"])

    def test_delete_groups_insert(self) -> None:
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        write_unprocessed_events(self.storage, [self.event])

        assert self._issue_count(self.project_id) == [{"count": 1, "group_id": 1}]

        timestamp = datetime.utcnow()

        project_id = self.project_id

        message: Message[KafkaPayload] = Message(
            BrokerValue(
                KafkaPayload(
                    None,
                    json.dumps(
                        (
                            2,
                            ReplacementType.END_DELETE_GROUPS,
                            {
                                "project_id": project_id,
                                "group_ids": [1],
                                "datetime": timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                            },
                        )
                    ).encode("utf-8"),
                    [],
                ),
                Partition(Topic("replacements"), 1),
                42,
                datetime.now(),
            )
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

        message = Message(
            BrokerValue(
                KafkaPayload(
                    None,
                    json.dumps(
                        (
                            2,
                            ReplacementType.TOMBSTONE_EVENTS,
                            {"project_id": project_id, "event_ids": [event_id]},
                        )
                    ).encode("utf-8"),
                    [],
                ),
                Partition(Topic("replacements"), 1),
                41,
                datetime.now(),
            )
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

        message = Message(
            BrokerValue(
                KafkaPayload(
                    None,
                    json.dumps(
                        (
                            2,
                            ReplacementType.EXCLUDE_GROUPS,
                            {"project_id": project_id, "group_ids": [1]},
                        )
                    ).encode("utf-8"),
                    [],
                ),
                Partition(Topic("replacements"), 1),
                42,
                datetime.now(),
            )
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

        timestamp = datetime.utcnow()

        project_id = self.project_id

        message = Message(
            BrokerValue(
                KafkaPayload(
                    None,
                    json.dumps(
                        (
                            2,
                            ReplacementType.END_MERGE,
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
                Partition(Topic("replacements"), 1),
                42,
                datetime.now(),
            )
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

        timestamp = datetime.utcnow()

        project_id = self.project_id

        message = Message(
            BrokerValue(
                KafkaPayload(
                    None,
                    json.dumps(
                        (
                            2,
                            ReplacementType.END_UNMERGE,
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
                Partition(Topic("replacements"), 1),
                42,
                datetime.now(),
            )
        )

        processed = self.replacer.process_message(message)
        self.replacer.flush_batch([processed])

        assert self._issue_count(self.project_id) == [{"count": 1, "group_id": 2}]

    def test_process_offset_twice(self) -> None:
        set_config("skip_seen_offsets", True)
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.event["primary_hash"] = "a" * 32
        write_unprocessed_events(self.storage, [self.event])

        message = Message(
            BrokerValue(
                KafkaPayload(
                    None,
                    json.dumps(
                        (
                            2,
                            ReplacementType.END_UNMERGE,
                            {
                                "project_id": self.project_id,
                                "previous_group_id": 1,
                                "new_group_id": 2,
                                "hashes": ["a" * 32],
                                "datetime": datetime.utcnow().strftime(
                                    PAYLOAD_DATETIME_FORMAT
                                ),
                            },
                        )
                    ).encode("utf-8"),
                    [],
                ),
                Partition(Topic("replacements"), 1),
                42,
                datetime.now(),
            )
        )

        processed = self.replacer.process_message(message)
        self.replacer.flush_batch([processed])

        # should be None since the offset should be in Redis, indicating it should be skipped
        assert self.replacer.process_message(message) is None

    def test_multiple_partitions(self) -> None:
        """
        Different partitions should have independent offset checks.
        """
        set_config("skip_seen_offsets", True)
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.event["primary_hash"] = "a" * 32
        write_unprocessed_events(self.storage, [self.event])

        payload = KafkaPayload(
            None,
            json.dumps(
                (
                    2,
                    ReplacementType.END_UNMERGE,
                    {
                        "project_id": self.project_id,
                        "previous_group_id": 1,
                        "new_group_id": 2,
                        "hashes": ["a" * 32],
                        "datetime": datetime.utcnow().strftime(PAYLOAD_DATETIME_FORMAT),
                    },
                )
            ).encode("utf-8"),
            [],
        )
        offset = 42
        timestamp = datetime.now()

        partition_one = Message(
            BrokerValue(
                payload,
                Partition(Topic("replacements"), 1),
                offset,
                timestamp,
            )
        )
        partition_two: Message[KafkaPayload] = Message(
            BrokerValue(
                payload,
                Partition(Topic("replacements"), 2),
                offset,
                timestamp,
            )
        )

        processed = self.replacer.process_message(partition_one)
        self.replacer.flush_batch([processed])
        # different partition should be unaffected even if it's the same offset
        assert self.replacer.process_message(partition_two) is not None

    def test_reset_consumer_group_offset_check(self) -> None:
        set_config("skip_seen_offsets", True)
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.event["primary_hash"] = "a" * 32
        write_unprocessed_events(self.storage, [self.event])

        message: Message[KafkaPayload] = Message(
            BrokerValue(
                KafkaPayload(
                    None,
                    json.dumps(
                        (
                            2,
                            ReplacementType.END_UNMERGE,
                            {
                                "project_id": self.project_id,
                                "previous_group_id": 1,
                                "new_group_id": 2,
                                "hashes": ["a" * 32],
                                "datetime": datetime.utcnow().strftime(
                                    PAYLOAD_DATETIME_FORMAT
                                ),
                            },
                        )
                    ).encode("utf-8"),
                    [],
                ),
                Partition(Topic("replacements"), 1),
                42,
                datetime.now(),
            )
        )

        self.replacer.flush_batch([self.replacer.process_message(message)])

        set_config(replacer.RESET_CHECK_CONFIG, f"[{CONSUMER_GROUP}]")

        # Offset to check against should be reset so this message shouldn't be skipped
        assert self.replacer.process_message(message) is not None

    def test_offset_already_processed(self) -> None:
        """
        Don't process an offset that already exists in Redis.
        """
        set_config("skip_seen_offsets", True)
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.event["primary_hash"] = "a" * 32
        write_unprocessed_events(self.storage, [self.event])

        key = f"replacement:{CONSUMER_GROUP}:errors:1"
        redis_client.set(key, 42)

        old_offset: Message[KafkaPayload] = Message(
            BrokerValue(
                KafkaPayload(
                    None,
                    json.dumps(
                        (
                            2,
                            ReplacementType.END_UNMERGE,
                            {},
                        )
                    ).encode("utf-8"),
                    [],
                ),
                Partition(Topic("replacements"), 1),
                41,
                datetime.now(),
            )
        )

        same_offset: Message[KafkaPayload] = Message(
            BrokerValue(
                KafkaPayload(
                    None,
                    json.dumps(
                        (
                            2,
                            ReplacementType.END_UNMERGE,
                            {},
                        )
                    ).encode("utf-8"),
                    [],
                ),
                Partition(Topic("replacements"), 1),
                42,
                datetime.now(),
            )
        )

        assert self.replacer.process_message(old_offset) is None
        assert self.replacer.process_message(same_offset) is None


@pytest.mark.redis_db
class TestReplacerProcess(BaseTest):
    def test_unmerge_hierarchical_process(self) -> None:
        timestamp = datetime.now()

        message = (
            2,
            ReplacementType.END_UNMERGE_HIERARCHICAL,
            {
                "project_id": self.project_id,
                "previous_group_id": 1,
                "new_group_id": 2,
                "hierarchical_hash": "a" * 32,
                "primary_hash": "b" * 32,
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement
        assert replacement is not None

        query_args = {
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, 2, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "primary_hash": "'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'",
            "hierarchical_hash": "toUUID('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')",
            "previous_group_id": 1,
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
            "table_name": "foo",
        }

        assert (
            re.sub("[\n ]+", " ", replacement.get_count_query("foo")).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE primary_hash = %(primary_hash)s WHERE group_id = %(previous_group_id)s AND has(hierarchical_hashes, %(hierarchical_hash)s) AND project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
            % query_args
        )
        assert (
            re.sub("[\n ]+", " ", replacement.get_insert_query("foo")).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE primary_hash = %(primary_hash)s WHERE group_id = %(previous_group_id)s AND has(hierarchical_hashes, %(hierarchical_hash)s) AND project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
            % query_args
        )

        assert replacement.get_query_time_flags() is None

    def test_delete_promoted_tag_process(self) -> None:
        timestamp = datetime.now()
        message = (
            2,
            ReplacementType.END_DELETE_TAG,
            {
                "project_id": self.project_id,
                "tag": "sentry:user",
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement
        assert replacement is not None

        query_args = {
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, '', user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, 'sentry:user')), `tags.key`), arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, 'sentry:user'), arrayEnumerate(`tags.value`))), contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "tag_str": "'sentry:user'",
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
            "table_name": "foo",
        }

        assert (
            re.sub("[\n ]+", " ", replacement.get_count_query("foo")).strip()
            == "SELECT count() FROM %(table_name)s FINAL WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted AND has(`tags.key`, %(tag_str)s)"
            % query_args
        )
        assert (
            re.sub("[\n ]+", " ", replacement.get_insert_query("foo")).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted AND has(`tags.key`, %(tag_str)s)"
            % query_args
        )
        assert replacement.get_query_time_flags() == errors_replacer.NeedsFinal()

    def test_delete_unpromoted_tag_process(self) -> None:
        timestamp = datetime.now()
        message = (
            2,
            ReplacementType.END_DELETE_TAG,
            {
                "project_id": self.project_id,
                "tag": "foo:bar",
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement
        assert replacement is not None

        query_args = {
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, 'foo:bar')), `tags.key`), arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, 'foo:bar'), arrayEnumerate(`tags.value`))), contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "tag_str": "'foo:bar'",
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
            "table_name": "foo",
        }

        assert (
            re.sub("[\n ]+", " ", replacement.get_count_query("foo")).strip()
            == "SELECT count() FROM %(table_name)s FINAL WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted AND has(`tags.key`, %(tag_str)s)"
            % query_args
        )
        assert (
            re.sub("[\n ]+", " ", replacement.get_insert_query("foo")).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted AND has(`tags.key`, %(tag_str)s)"
            % query_args
        )

        assert replacement.get_query_time_flags() == errors_replacer.NeedsFinal()

    @pytest.mark.parametrize(
        "old_primary_hash", ["e3d704f3542b44a621ebed70dc0efe13", False, None]
    )
    def test_tombstone_events_process(self, old_primary_hash) -> None:
        timestamp = datetime.now()
        message_kwargs = {
            "project_id": self.project_id,
            "event_ids": ["00e24a150d7f4ee4b142b61b4d893b6d"],
            "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }

        if old_primary_hash is not None:
            message_kwargs["old_primary_hash"] = old_primary_hash

        message = (2, ReplacementType.TOMBSTONE_EVENTS, message_kwargs)

        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement

        old_primary_condition = (
            " AND primary_hash = 'e3d704f3-542b-44a6-21eb-ed70dc0efe13'"
            if old_primary_hash
            else ""
        )

        query_args = {
            "event_ids": "'00e24a15-0d7f-4ee4-b142-b61b4d893b6d'",
            "project_id": str(self.project_id),
            "required_columns": "event_id, primary_hash, project_id, group_id, timestamp, deleted, retention_days",
            "select_columns": "event_id, primary_hash, project_id, group_id, timestamp, 1, retention_days",
            "table_name": "foo",
        }

        assert (
            re.sub("[\n ]+", " ", replacement.get_count_query("foo")).strip()
            == f"SELECT count() FROM %(table_name)s FINAL PREWHERE event_id IN (%(event_ids)s){old_primary_condition} WHERE project_id = %(project_id)s AND NOT deleted"
            % query_args
        )
        assert (
            re.sub("[\n ]+", " ", replacement.get_insert_query("foo")).strip()
            == f"INSERT INTO %(table_name)s (%(required_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE event_id IN (%(event_ids)s){old_primary_condition} WHERE project_id = %(project_id)s AND NOT deleted"
            % query_args
        )

        assert replacement.get_query_time_flags() is None

    def test_replace_group_process(self) -> None:
        timestamp = datetime.now()
        message = (
            2,
            ReplacementType.REPLACE_GROUP,
            {
                "project_id": self.project_id,
                "event_ids": ["00e24a150d7f4ee4b142b61b4d893b6d"],
                "new_group_id": 2,
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement
        assert replacement is not None

        query_args = {
            "event_ids": "'00e24a15-0d7f-4ee4-b142-b61b4d893b6d'",
            "project_id": self.project_id,
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, 2, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "table_name": "foo",
        }

        assert (
            re.sub("[\n ]+", " ", replacement.get_count_query("foo")).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE event_id IN (%(event_ids)s) WHERE project_id = %(project_id)s AND NOT deleted"
            % query_args
        )

        assert (
            re.sub("[\n ]+", " ", replacement.get_insert_query("foo")).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE event_id IN (%(event_ids)s) WHERE project_id = %(project_id)s AND NOT deleted"
            % query_args
        )
        assert replacement.get_query_time_flags() is None

    def test_merge_process(self) -> None:
        timestamp = datetime.now()
        message = (
            2,
            ReplacementType.END_MERGE,
            {
                "project_id": self.project_id,
                "new_group_id": 2,
                "previous_group_ids": [1, 2],
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement
        assert replacement is not None

        query_args = {
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, 2, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "previous_group_ids": ", ".join(str(gid) for gid in [1, 2]),
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
            "table_name": "foo",
        }

        assert (
            re.sub("[\n ]+", " ", replacement.get_count_query("foo")).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE group_id IN (%(previous_group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
            % query_args
        )
        assert (
            re.sub("[\n ]+", " ", replacement.get_insert_query("foo")).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE group_id IN (%(previous_group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
            % query_args
        )

        assert replacement.get_query_time_flags() == errors_replacer.ExcludeGroups(
            [1, 2]
        )

    def test_unmerge_process(self) -> None:
        timestamp = datetime.now()
        message = (
            2,
            ReplacementType.END_UNMERGE,
            {
                "project_id": self.project_id,
                "previous_group_id": 1,
                "new_group_id": 2,
                "hashes": ["a" * 32, "b" * 32],
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement

        query_args = {
            "all_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "select_columns": "project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, contexts.key, contexts.value, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, 2, primary_hash, hierarchical_hashes, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, exception_main_thread, sdk_integrations, modules.name, modules.version",
            "hashes": "'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'",
            "previous_group_id": 1,
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
            "table_name": "foo",
        }

        assert (
            re.sub("[\n ]+", " ", replacement.get_count_query("foo")).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE primary_hash IN (%(hashes)s) WHERE group_id = %(previous_group_id)s AND project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
            % query_args
        )
        assert (
            re.sub("[\n ]+", " ", replacement.get_insert_query("foo")).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE primary_hash IN (%(hashes)s) WHERE group_id = %(previous_group_id)s AND project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
            % query_args
        )

        assert replacement.get_query_time_flags() == errors_replacer.NeedsFinal()

    def test_tombstone_events_process_timestamp(self) -> None:
        from_ts = datetime.now()
        to_ts = datetime.now() + timedelta(3)
        message = (
            2,
            ReplacementType.TOMBSTONE_EVENTS,
            {
                "project_id": self.project_id,
                "event_ids": ["00e24a150d7f4ee4b142b61b4d893b6d"],
                "from_timestamp": from_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "to_timestamp": to_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement
        assert replacement is not None

        query_args = {
            "event_ids": "'00e24a15-0d7f-4ee4-b142-b61b4d893b6d'",
            "project_id": str(self.project_id),
            "required_columns": "event_id, primary_hash, project_id, group_id, timestamp, deleted, retention_days",
            "select_columns": "event_id, primary_hash, project_id, group_id, timestamp, 1, retention_days",
            "table_name": "foo",
        }

        assert (
            re.sub("[\n ]+", " ", replacement.get_count_query("foo")).strip()
            == f"SELECT count() FROM %(table_name)s FINAL PREWHERE event_id IN (%(event_ids)s) WHERE project_id = %(project_id)s AND NOT deleted AND timestamp >= toDateTime('{from_ts.strftime(DATETIME_FORMAT)}') AND timestamp <= toDateTime('{to_ts.strftime(DATETIME_FORMAT)}')"
            % query_args
        )
        assert (
            re.sub("[\n ]+", " ", replacement.get_insert_query("foo")).strip()
            == f"INSERT INTO %(table_name)s (%(required_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE event_id IN (%(event_ids)s) WHERE project_id = %(project_id)s AND NOT deleted AND timestamp >= toDateTime('{from_ts.strftime(DATETIME_FORMAT)}') AND timestamp <= toDateTime('{to_ts.strftime(DATETIME_FORMAT)}')"
            % query_args
        )
        assert replacement.get_query_time_flags() is None

    def test_delete_groups_process(self) -> None:
        timestamp = datetime.now()
        message = (
            2,
            ReplacementType.END_DELETE_GROUPS,
            {
                "project_id": self.project_id,
                "group_ids": [1, 2, 3],
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement
        assert replacement is not None
        query_args = {
            "group_ids": "1, 2, 3",
            "project_id": self.project_id,
            "required_columns": "event_id, primary_hash, project_id, group_id, timestamp, deleted, retention_days",
            "select_columns": "event_id, primary_hash, project_id, group_id, timestamp, 1, retention_days",
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
            "table_name": "foo",
        }
        assert (
            re.sub("[\n ]+", " ", replacement.get_count_query("foo")).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE group_id IN (%(group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
            % query_args
        )
        assert (
            re.sub("[\n ]+", " ", replacement.get_insert_query("foo")).strip()
            == "INSERT INTO %(table_name)s (%(required_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE group_id IN (%(group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
            % query_args
        )
        assert replacement.get_project_id() == self.project_id
        assert replacement.get_query_time_flags() == errors_replacer.ExcludeGroups(
            group_ids=[1, 2, 3]
        )

    def test_project_bypass(self) -> None:
        timestamp = datetime.now()
        message = (
            2,
            ReplacementType.END_DELETE_GROUPS,
            {
                "project_id": self.project_id,
                "group_ids": [1, 2, 3],
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )
        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement
        assert replacement is not None

        set_config("replacements_bypass_projects", f"[{self.project_id + 1}]")
        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is not None
        _, replacement = meta_and_replacement
        assert replacement is not None

        set_config(
            "replacements_bypass_projects", f"[{self.project_id + 1},{self.project_id}]"
        )
        meta_and_replacement = self.replacer.process_message(self._wrap(message))
        assert meta_and_replacement is None
        delete_config("replacements_bypass_projects")
