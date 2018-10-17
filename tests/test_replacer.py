import pytz
import re
from datetime import datetime
import simplejson as json

from base import BaseTest

from snuba import settings
from snuba import replacer
from snuba.processor import InvalidMessageType, InvalidMessageVersion, process_message


class TestReplacer(BaseTest):
    def setup_method(self, test_method):
        super(TestReplacer, self).setup_method(test_method)
        self.replacer = replacer.ReplacerWorker(self.clickhouse, self.table)

    def _wrap(self, msg):
        class FakeKafkaMessage(object):
            def __init__(self, msg):
                self.msg = msg

            def value(self):
                return json.dumps(self.msg).encode('utf-8')

        return FakeKafkaMessage(msg)

    def test_delete_groups(self):
        timestamp = datetime.now(tz=pytz.utc)
        message = (2, 'end_delete_groups', {
            'project_id': 1,
            'group_ids': [1, 2, 3],
            'datetime': timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        })

        count_query_template, insert_query_template, query_args = self.replacer.process_message(self._wrap(message))

        assert re.sub("[\n ]+", " ", count_query_template).strip() == \
            "SELECT count() FROM %(dist_table_name)s WHERE project_id = %(project_id)s AND group_id IN (%(group_ids)s) AND timestamp <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        assert re.sub("[\n ]+", " ", insert_query_template).strip() == \
            "INSERT INTO %(dist_table_name)s (%(required_columns)s) SELECT %(select_columns)s FROM %(dist_table_name)s WHERE project_id = %(project_id)s AND group_id IN (%(group_ids)s) AND timestamp <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        assert query_args == {
            'group_ids': '1, 2, 3',
            'project_id': 1,
            'required_columns': 'event_id, project_id, group_id, timestamp, deleted, retention_days',
            'select_columns': 'event_id, project_id, group_id, timestamp, 1, retention_days',
            'timestamp': timestamp.strftime(replacer.CLICKHOUSE_DATETIME_FORMAT),
        }

    def test_merge(self):
        timestamp = datetime.now(tz=pytz.utc)
        message = (2, 'end_merge', {
            'project_id': 1,
            'new_group_id': 2,
            'previous_group_ids': [1, 2],
            'datetime': timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        })

        count_query_template, insert_query_template, query_args = self.replacer.process_message(self._wrap(message))

        assert re.sub("[\n ]+", " ", count_query_template).strip() == \
            "SELECT count() FROM %(dist_table_name)s WHERE project_id = %(project_id)s AND group_id IN (%(previous_group_ids)s) AND timestamp <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        assert re.sub("[\n ]+", " ", insert_query_template).strip() == \
            "INSERT INTO %(dist_table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(dist_table_name)s WHERE project_id = %(project_id)s AND group_id IN (%(previous_group_ids)s) AND timestamp <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        assert query_args == {
            'all_columns': 'event_id, project_id, group_id, timestamp, deleted, retention_days, platform, message, primary_hash, received, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, tags.key, tags.value, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level',
            'select_columns': 'event_id, project_id, 2, timestamp, deleted, retention_days, platform, message, primary_hash, received, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, tags.key, tags.value, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level',
            'previous_group_ids': ", ".join(str(gid) for gid in [1, 2]),
            'project_id': 1,
            'timestamp': timestamp.strftime(replacer.CLICKHOUSE_DATETIME_FORMAT),
        }

    def test_unmerge(self):
        timestamp = datetime.now(tz=pytz.utc)
        message = (2, 'end_unmerge', {
            'project_id': 1,
            'previous_group_id': 1,
            'new_group_id': 2,
            'hashes': ["a" * 32, "b" * 32],
            'datetime': timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        })

        count_query_template, insert_query_template, query_args = self.replacer.process_message(self._wrap(message))

        assert re.sub("[\n ]+", " ", count_query_template).strip() == \
            "SELECT count() FROM %(dist_table_name)s WHERE project_id = %(project_id)s AND group_id = %(previous_group_id)s AND primary_hash IN (%(hashes)s) AND timestamp <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        assert re.sub("[\n ]+", " ", insert_query_template).strip() == \
            "INSERT INTO %(dist_table_name)s (%(all_columns)s) SELECT %(select_columns)s FROM %(dist_table_name)s WHERE project_id = %(project_id)s AND group_id = %(previous_group_id)s AND primary_hash IN (%(hashes)s) AND timestamp <= CAST('%(timestamp)s' AS DateTime) AND NOT deleted"
        assert query_args == {
            'all_columns': 'event_id, project_id, group_id, timestamp, deleted, retention_days, platform, message, primary_hash, received, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, tags.key, tags.value, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level',
            'select_columns': 'event_id, project_id, 2, timestamp, deleted, retention_days, platform, message, primary_hash, received, user_id, username, email, ip_address, geo_country_code, geo_region, geo_city, sdk_name, sdk_version, type, version, offset, partition, os_build, os_kernel_version, device_name, device_brand, device_locale, device_uuid, device_model_id, device_arch, device_battery_level, device_orientation, device_simulator, device_online, device_charging, level, logger, server_name, transaction, environment, `sentry:release`, `sentry:dist`, `sentry:user`, site, url, app_device, device, device_family, runtime, runtime_name, browser, browser_name, os, os_name, os_rooted, tags.key, tags.value, contexts.key, contexts.value, http_method, http_referer, exception_stacks.type, exception_stacks.value, exception_stacks.mechanism_type, exception_stacks.mechanism_handled, exception_frames.abs_path, exception_frames.filename, exception_frames.package, exception_frames.module, exception_frames.function, exception_frames.in_app, exception_frames.colno, exception_frames.lineno, exception_frames.stack_level',
            'hashes': "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'",
            'previous_group_id': 1,
            'project_id': 1,
            'timestamp': timestamp.strftime(replacer.CLICKHOUSE_DATETIME_FORMAT),
        }
