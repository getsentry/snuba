import importlib
import re
from datetime import datetime, timedelta
from functools import partial

import pytz
import simplejson as json

from snuba import replacer, settings
from snuba.clickhouse import DATETIME_FORMAT
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets import errors_replacer
from snuba.datasets.errors_replacer import ReplacerState
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
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
        self.storage = get_storage(StorageKey.EVENTS)

        self.replacer = replacer.ReplacerWorker(
            self.storage, DummyMetricsBackend(strict=True)
        )

        self.project_id = 1
        self.event = get_raw_event()
        settings.ERRORS_ROLLOUT_ALL = False
        settings.ERRORS_ROLLOUT_WRITABLE_STORAGE = False

    def teardown_method(self):
        importlib.reload(settings)

    def _wrap(self, msg: str) -> Message[KafkaPayload]:
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

        return json.loads(self.app.post("/query", data=json.dumps(args)).data)["data"]

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
            "required_columns": "event_id, project_id, group_id, timestamp, deleted, retention_days",
            "select_columns": "event_id, project_id, group_id, timestamp, 1, retention_days",
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
        }
        assert replacement.query_time_flags == (
            errors_replacer.EXCLUDE_GROUPS,
            self.project_id,
            [1, 2, 3],
        )

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
            "all_columns": "event_id, project_id, group_id, timestamp, deleted, retention_days, platform, message, primary_hash, hierarchical_hashes, received, search_message, title, location, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, message_timestamp, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level, culprit, sdk_integrations, modules.name, modules.version",
            "select_columns": "event_id, project_id, 2, timestamp, deleted, retention_days, platform, message, primary_hash, hierarchical_hashes, received, search_message, title, location, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, message_timestamp, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level, culprit, sdk_integrations, modules.name, modules.version",
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
            "all_columns": "event_id, project_id, group_id, timestamp, deleted, retention_days, platform, message, primary_hash, hierarchical_hashes, received, search_message, title, location, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, message_timestamp, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level, culprit, sdk_integrations, modules.name, modules.version",
            "select_columns": "event_id, project_id, 2, timestamp, deleted, retention_days, platform, message, primary_hash, hierarchical_hashes, received, search_message, title, location, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, message_timestamp, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level, culprit, sdk_integrations, modules.name, modules.version",
            "hashes": "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'",
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
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE %(tag_column)s IS NOT NULL WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE %(tag_column)s IS NOT NULL WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert replacement.query_args == {
            "all_columns": "event_id, project_id, group_id, timestamp, deleted, retention_days, platform, message, primary_hash, hierarchical_hashes, received, search_message, title, location, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, message_timestamp, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level, culprit, sdk_integrations, modules.name, modules.version",
            "select_columns": "event_id, project_id, group_id, timestamp, deleted, retention_days, platform, message, primary_hash, hierarchical_hashes, received, search_message, title, location, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, message_timestamp, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, NULL, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, 'sentry:user')), `tags.key`), arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, 'sentry:user'), arrayEnumerate(`tags.value`))), _tags_flattened, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level, culprit, sdk_integrations, modules.name, modules.version",
            "tag_column": "`sentry:user`",
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
            "all_columns": "event_id, project_id, group_id, timestamp, deleted, retention_days, platform, message, primary_hash, hierarchical_hashes, received, search_message, title, location, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, message_timestamp, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level, culprit, sdk_integrations, modules.name, modules.version",
            "select_columns": "event_id, project_id, group_id, timestamp, deleted, retention_days, platform, message, primary_hash, hierarchical_hashes, received, search_message, title, location, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, message_timestamp, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, 'foo:bar')), `tags.key`), arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, 'foo:bar'), arrayEnumerate(`tags.value`))), _tags_flattened, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level, culprit, sdk_integrations, modules.name, modules.version",
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

    def test_delete_tag_promoted_insert(self) -> None:
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.event["data"]["tags"].append(["browser.name", "foo"])
        self.event["data"]["tags"].append(["notbrowser", "foo"])
        write_unprocessed_events(self.storage, [self.event])

        project_id = self.project_id

        def _issue_count(total=False):
            return json.loads(
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": [project_id],
                            "aggregations": [["count()", "", "count"]],
                            "conditions": [["tags[browser.name]", "=", "foo"]]
                            if not total
                            else [],
                            "groupby": ["group_id"],
                        }
                    ),
                ).data
            )["data"]

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
                        "end_delete_tag",
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
        self.replacer.flush_batch([processed])

        assert _issue_count() == []
        assert _issue_count(total=True) == [{"count": 1, "group_id": 1}]

    def test_query_time_flags(self) -> None:
        project_ids = [1, 2]

        assert errors_replacer.get_projects_query_flags(
            project_ids, ReplacerState.ERRORS
        ) == (False, [],)

        errors_replacer.set_project_needs_final(100, ReplacerState.ERRORS)
        assert errors_replacer.get_projects_query_flags(
            project_ids, ReplacerState.ERRORS
        ) == (False, [],)

        errors_replacer.set_project_needs_final(1, ReplacerState.ERRORS)
        assert errors_replacer.get_projects_query_flags(
            project_ids, ReplacerState.ERRORS
        ) == (True, [],)
        assert errors_replacer.get_projects_query_flags(
            project_ids, ReplacerState.EVENTS
        ) == (False, [],)

        errors_replacer.set_project_needs_final(2, ReplacerState.ERRORS)
        assert errors_replacer.get_projects_query_flags(
            project_ids, ReplacerState.ERRORS
        ) == (True, [],)

        errors_replacer.set_project_exclude_groups(1, [1, 2], ReplacerState.ERRORS)
        errors_replacer.set_project_exclude_groups(2, [3, 4], ReplacerState.ERRORS)
        assert errors_replacer.get_projects_query_flags(
            project_ids, ReplacerState.ERRORS
        ) == (True, [1, 2, 3, 4],)
        assert errors_replacer.get_projects_query_flags(
            project_ids, ReplacerState.EVENTS
        ) == (False, [],)

    def test_tombstone_events_process_noop(self) -> None:
        timestamp = datetime.now(tz=pytz.utc)
        message = (
            2,
            "tombstone_events",
            {
                "project_id": self.project_id,
                "event_ids": ["00e24a150d7f4ee4b142b61b4d893b6d"],
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "old_primary_hash": "e3d704f3542b44a621ebed70dc0efe13",
            },
        )

        assert self.replacer.process_message(self._wrap(message)) is None

    def test_tombstone_events_process(self) -> None:
        timestamp = datetime.now(tz=pytz.utc)
        message = (
            2,
            "tombstone_events",
            {
                "project_id": self.project_id,
                "event_ids": ["00e24a150d7f4ee4b142b61b4d893b6d"],
                "datetime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        replacement = self.replacer.process_message(self._wrap(message))

        assert replacement is not None

        assert (
            re.sub("[\n ]+", " ", replacement.count_query_template).strip()
            == "SELECT count() FROM %(table_name)s FINAL PREWHERE cityHash64(toString(event_id)) IN (%(event_ids)s) WHERE project_id = %(project_id)s AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(table_name)s (%(required_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE cityHash64(toString(event_id)) IN (%(event_ids)s) WHERE project_id = %(project_id)s AND NOT deleted"
        )

        assert replacement.query_args == {
            "event_ids": "cityHash64('00e24a150d7f4ee4b142b61b4d893b6d')",
            "project_id": self.project_id,
            "required_columns": "event_id, project_id, group_id, timestamp, deleted, retention_days",
            "select_columns": "event_id, project_id, group_id, timestamp, 1, retention_days",
            "old_primary_hash": "NULL",
        }

        assert replacement.query_time_flags == (None, self.project_id,)

    def test_tombstone_events_process_timestamp(self) -> None:
        from_ts = datetime.now(tz=pytz.utc)
        to_ts = datetime.now(tz=pytz.utc) + timedelta(3)
        message = (
            2,
            "tombstone_events",
            {
                "project_id": self.project_id,
                "event_ids": ["00e24a150d7f4ee4b142b61b4d893b6d"],
                "from_timestamp": from_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "to_timestamp": to_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        )

        replacement = self.replacer.process_message(self._wrap(message))

        assert replacement is not None

        assert (
            re.sub("[\n ]+", " ", replacement.count_query_template).strip()
            == f"SELECT count() FROM %(table_name)s FINAL PREWHERE cityHash64(toString(event_id)) IN (%(event_ids)s) WHERE project_id = %(project_id)s AND timestamp >= toDateTime('{from_ts.strftime(DATETIME_FORMAT)}') AND timestamp <= toDateTime('{to_ts.strftime(DATETIME_FORMAT)}') AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == f"INSERT INTO %(table_name)s (%(required_columns)s) SELECT %(select_columns)s FROM %(table_name)s FINAL PREWHERE cityHash64(toString(event_id)) IN (%(event_ids)s) WHERE project_id = %(project_id)s AND timestamp >= toDateTime('{from_ts.strftime(DATETIME_FORMAT)}') AND timestamp <= toDateTime('{to_ts.strftime(DATETIME_FORMAT)}') AND NOT deleted"
        )

        assert replacement.query_args == {
            "event_ids": "cityHash64('00e24a150d7f4ee4b142b61b4d893b6d')",
            "project_id": self.project_id,
            "required_columns": "event_id, project_id, group_id, timestamp, deleted, retention_days",
            "select_columns": "event_id, project_id, group_id, timestamp, 1, retention_days",
            "old_primary_hash": "NULL",
        }

        assert replacement.query_time_flags == (None, self.project_id,)
