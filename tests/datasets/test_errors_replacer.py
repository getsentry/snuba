import pytz
import re
from datetime import datetime
from functools import partial
import simplejson as json

from snuba import replacer
from snuba.clickhouse import DATETIME_FORMAT
from snuba.datasets.errors_replacer import FLATTENED_COLUMN_TEMPLATE
from snuba.datasets import errors_replacer
from snuba.settings import PAYLOAD_DATETIME_FORMAT
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams import Message, Partition, Topic
from snuba.utils.streams.backends.kafka import KafkaPayload
from tests.base import BaseEventsTest


class TestReplacer(BaseEventsTest):
    def setup_method(self, test_method):
        super(TestReplacer, self).setup_method(test_method, "events_migration")

        from snuba.web.views import application

        assert application.testing is True

        self.app = application.test_client()
        self.app.post = partial(self.app.post, headers={"referer": "test"})

        self.replacer = replacer.ReplacerWorker(
            self.dataset.get_writable_storage(), DummyMetricsBackend(strict=True)
        )

        self.project_id = 1

    def _wrap(self, msg: str) -> Message[KafkaPayload]:
        return Message(
            Partition(Topic("replacements"), 0),
            0,
            KafkaPayload(None, json.dumps(msg).encode("utf-8"), []),
            datetime.now(),
        )

    def _issue_count(self, project_id, group_id=None):
        args = {
            "project": [project_id],
            "aggregations": [["count()", "", "count"]],
            "groupby": ["group_id"],
        }

        if group_id:
            args.setdefault("conditions", []).append(("group_id", "=", group_id))

        return json.loads(
            self.app.post("/events_migration/query", data=json.dumps(args)).data
        )["data"]

    def test_delete_groups_process(self):
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
            == "SELECT count() FROM %(dist_read_table_name)s FINAL PREWHERE group_id IN (%(group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(dist_write_table_name)s (%(required_columns)s) SELECT %(select_columns)s FROM %(dist_read_table_name)s FINAL PREWHERE group_id IN (%(group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
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

    def test_merge_process(self):
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
            == "SELECT count() FROM %(dist_read_table_name)s FINAL PREWHERE group_id IN (%(previous_group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(dist_write_table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(dist_read_table_name)s FINAL PREWHERE group_id IN (%(previous_group_ids)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert replacement.query_args == {
            "all_columns": "org_id, project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, _contexts_flattened, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, event_string, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "select_columns": "org_id, project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, _contexts_flattened, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, 2, primary_hash, event_string, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "previous_group_ids": ", ".join(str(gid) for gid in [1, 2]),
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
        }
        assert replacement.query_time_flags == (
            errors_replacer.EXCLUDE_GROUPS,
            self.project_id,
            [1, 2],
        )

    def test_unmerge_process(self):
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
            == "SELECT count() FROM %(dist_read_table_name)s FINAL PREWHERE group_id = %(previous_group_id)s WHERE project_id = %(project_id)s AND primary_hash IN (%(hashes)s) AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(dist_write_table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(dist_read_table_name)s FINAL PREWHERE group_id = %(previous_group_id)s WHERE project_id = %(project_id)s AND primary_hash IN (%(hashes)s) AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert replacement.query_args == {
            "all_columns": "org_id, project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, _contexts_flattened, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, event_string, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "select_columns": "org_id, project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, _contexts_flattened, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, 2, primary_hash, event_string, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "hashes": "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'",
            "previous_group_id": 1,
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
        }
        assert replacement.query_time_flags == (
            errors_replacer.NEEDS_FINAL,
            self.project_id,
        )

    def test_delete_promoted_tag_process(self):
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
            == "SELECT count() FROM %(dist_read_table_name)s FINAL PREWHERE %(tag_column)s IS NOT NULL WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(dist_write_table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(dist_read_table_name)s FINAL PREWHERE %(tag_column)s IS NOT NULL WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        flattened_column = FLATTENED_COLUMN_TEMPLATE % "'sentry:user'"
        assert replacement.query_args == {
            "all_columns": "org_id, project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, _contexts_flattened, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, event_string, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "select_columns": f"org_id, project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, NULL, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, 'sentry:user')), `tags.key`), arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, 'sentry:user'), arrayEnumerate(`tags.value`))), {flattened_column}, contexts.key, contexts.value, _contexts_flattened, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, event_string, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "tag_column": "user",
            "tag_str": "'sentry:user'",
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
        }
        assert replacement.query_time_flags == (
            errors_replacer.NEEDS_FINAL,
            self.project_id,
        )

    def test_delete_unpromoted_tag_process(self):
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
            == "SELECT count() FROM %(dist_read_table_name)s FINAL PREWHERE has(`tags.key`, %(tag_str)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )
        assert (
            re.sub("[\n ]+", " ", replacement.insert_query_template).strip()
            == "INSERT INTO %(dist_write_table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(dist_read_table_name)s FINAL PREWHERE has(`tags.key`, %(tag_str)s) WHERE project_id = %(project_id)s AND received <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        )

        flattened_column = FLATTENED_COLUMN_TEMPLATE % "'foo:bar'"
        assert replacement.query_args == {
            "all_columns": "org_id, project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, tags.key, tags.value, _tags_flattened, contexts.key, contexts.value, _contexts_flattened, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, event_string, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "select_columns": f"org_id, project_id, timestamp, event_id, platform, environment, release, dist, ip_address_v4, ip_address_v6, user, user_id, user_name, user_email, sdk_name, sdk_version, http_method, http_referer, arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, 'foo:bar')), `tags.key`), arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, 'foo:bar'), arrayEnumerate(`tags.value`))), {flattened_column}, contexts.key, contexts.value, _contexts_flattened, transaction_name, span_id, trace_id, partition, offset, message_timestamp, retention_days, deleted, group_id, primary_hash, event_string, received, message, title, culprit, level, location, version, type, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.colno, exception_frames.filename, exception_frames.function, exception_frames.lineno, exception_frames.in_app, exception_frames.package, exception_frames.module, exception_frames.stack_level, sdk_integrations, modules.name, modules.version",
            "tag_column": "`foo:bar`",
            "tag_str": "'foo:bar'",
            "project_id": self.project_id,
            "timestamp": timestamp.strftime(DATETIME_FORMAT),
        }
        assert replacement.query_time_flags == (
            errors_replacer.NEEDS_FINAL,
            self.project_id,
        )

    def test_delete_groups_insert(self):
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.write_events([self.event])

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

    def test_merge_insert(self):
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.write_events([self.event])

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

    def test_unmerge_insert(self):
        self.event["project_id"] = self.project_id
        self.event["group_id"] = 1
        self.event["primary_hash"] = "a" * 32
        self.write_events([self.event])

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
