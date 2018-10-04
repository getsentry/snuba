import calendar
import pytest
import pytz
import re
from datetime import datetime

from base import BaseTest

from snuba import processor, settings
from snuba.processor import InvalidMessageType, InvalidMessageVersion, process_message


class TestProcessor(BaseTest):
    def test_simple(self):
        _, processed = process_message(self.event)

        for field in ('event_id', 'project_id', 'message', 'platform'):
            assert processed[field] == self.event[field]

    def test_simple_version_0(self):
        _, processed = process_message((0, 'insert', self.event))

        for field in ('event_id', 'project_id', 'message', 'platform'):
            assert processed[field] == self.event[field]

    def test_simple_version_1(self):
        assert process_message((0, 'insert', self.event)) == process_message((1, 'insert', self.event, {}))

    def test_invalid_type_version_0(self):
        with pytest.raises(InvalidMessageType):
            process_message((0, 'invalid', self.event))

    def test_invalid_version(self):
        with pytest.raises(InvalidMessageVersion):
            process_message((2 ** 32 - 1, 'insert', self.event))

    def test_invalid_format(self):
        with pytest.raises(InvalidMessageVersion):
            process_message((-1, 'insert', self.event))

    def test_unexpected_obj(self):
        self.event['message'] = {'what': 'why is this in the message'}

        _, processed = process_message(self.event)

        assert processed['message'] == '{"what": "why is this in the message"}'

    def test_hash_invalid_primary_hash(self):
        self.event['primary_hash'] = b"'tinymce' \u063a\u064a\u0631 \u0645\u062d".decode('unicode-escape')

        _, processed = process_message(self.event)

        assert processed['primary_hash'] == 'a52ccc1a61c2258e918b43b5aff50db1'

    def test_extract_required(self):
        now = datetime.utcnow()
        event = {
            'event_id': '1' * 32,
            'project_id': 100,
            'group_id': 10,
            'datetime': now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        output = {}

        processor.extract_required(output, event)
        assert output == {
            'event_id': '11111111111111111111111111111111',
            'project_id': 100,
            'group_id': 10,
            'timestamp': now,
            'retention_days': settings.DEFAULT_RETENTION_DAYS,
        }

    def test_extract_common(self):
        now = datetime.utcnow().replace(microsecond=0)
        event = {
            'primary_hash': 'a' * 32,
            'message': 'the message',
            'platform': 'the_platform',
        }
        data = {
            'received': int(calendar.timegm(now.timetuple())),
            'type': 'error',
            'version': 6,
        }
        output = {}

        processor.extract_common(output, event, data)
        assert output == {
            'message': u'the message',
            'platform': u'the_platform',
            'primary_hash': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'received': now,
            'type': 'error',
            'version': '6',
        }

    # def test_delete_groups(self):
    #     timestamp = datetime.now(tz=pytz.utc)
    #     message = (0, 'delete_groups', {
    #         'project_id': 1,
    #         'group_ids': [1, 2, 3],
    #         'datetime': timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
    #     })

    #     action_type, processed = processor.process_message(message)
    #     assert action_type is processor.ALTER

    #     query, args = processed
    #     assert re.sub("[\n ]+", " ", query).strip() == \
    #         "ALTER TABLE %(local_table_name)s UPDATE deleted = 1 WHERE project_id = %(project_id)s AND group_id IN (%(group_ids)s) AND timestamp <= CAST('%(timestamp)s' AS DateTime)"
    #     assert args == {
    #         'group_ids': '1, 2, 3',
    #         'project_id': 1,
    #         'timestamp': timestamp.strftime(processor.CLICKHOUSE_DATETIME_FORMAT),
    #     }

    # def test_unmerge(self):
    #     timestamp = datetime.now(tz=pytz.utc)
    #     message = (0, 'unmerge', {
    #         'project_id': 1,
    #         'new_group_id': 2,
    #         'event_ids': ["a" * 32, "b" * 32],
    #         'datetime': timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
    #     })

    #     action_type, processed = processor.process_message(message)
    #     assert action_type is processor.ALTER

    #     query, args = processed
    #     assert re.sub("[\n ]+", " ", query).strip() == \
    #         "ALTER TABLE %(local_table_name)s UPDATE group_id = %(new_group_id)s WHERE project_id = %(project_id)s AND event_id IN (%(event_ids)s) AND timestamp <= CAST('%(timestamp)s' AS DateTime)"
    #     assert args == {
    #         'event_ids': "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'",
    #         'new_group_id': 2,
    #         'project_id': 1,
    #         'timestamp': timestamp.strftime(processor.CLICKHOUSE_DATETIME_FORMAT),
    #     }

    # def test_merge(self):
    #     timestamp = datetime.now(tz=pytz.utc)
    #     message = (0, 'merge', {
    #         'project_id': 1,
    #         'new_group_id': 2,
    #         'previous_group_id': 1,
    #         'datetime': timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
    #     })

    #     action_type, processed = processor.process_message(message)
    #     assert action_type is processor.ALTER

    #     query, args = processed
    #     assert re.sub("[\n ]+", " ", query).strip() == \
    #         "ALTER TABLE %(local_table_name)s UPDATE group_id = %(new_group_id)s WHERE project_id = %(project_id)s AND group_id = %(previous_group_id)s AND timestamp <= CAST('%(timestamp)s' AS DateTime)"
    #     assert args == {
    #         'new_group_id': 2,
    #         'previous_group_id': 1,
    #         'project_id': 1,
    #         'timestamp': timestamp.strftime(processor.CLICKHOUSE_DATETIME_FORMAT),
    #     }

    def test_extract_sdk(self):
        sdk = {
            'integrations': ['logback'],
            'name': 'sentry-java',
            'version': '1.6.1-d1e3a'
        }
        output = {}

        processor.extract_sdk(output, sdk)

        assert output == {'sdk_name': u'sentry-java', 'sdk_version': u'1.6.1-d1e3a'}

    def test_extract_tags(self):
        orig_tags = {
            'sentry:user': 'the_user',
            'level': 'the_level',
            'logger': 'the_logger',
            'server_name': 'the_servername',
            'transaction': 'the_transaction',
            'environment': 'the_enviroment',
            'sentry:release': 'the_release',
            'sentry:dist': 'the_dist',
            'site': 'the_site',
            'url': 'the_url',
            'extra_tag': 'extra_value',
            'null_tag': None,
        }
        tags = orig_tags.copy()
        output = {}

        processor.extract_promoted_tags(output, tags)

        assert output == {
            'sentry:dist': 'the_dist',
            'environment': u'the_enviroment',
            'level': u'the_level',
            'logger': u'the_logger',
            'sentry:release': 'the_release',
            'server_name': u'the_servername',
            'site': u'the_site',
            'transaction': u'the_transaction',
            'url': u'the_url',
            'sentry:user': u'the_user',
        }
        assert tags == orig_tags

        extra_output = {}
        processor.extract_extra_tags(extra_output, tags)

        valid_items = [(k, v) for k, v in sorted(orig_tags.items()) if v]
        assert extra_output == {
            'tags.key': [k for k, v in valid_items],
            'tags.value': [v for k, v in valid_items]
        }

    def test_extract_tags_empty_string(self):
        # verify our text field extraction doesn't coerce '' to None
        tags = {
            'environment': '',
        }
        output = {}

        processor.extract_promoted_tags(output, tags)

        assert output['environment'] == u''

    def test_extract_contexts(self):
        contexts = {
            'app': {
                'device_app_hash': 'the_app_device_uuid',
            },
            'os': {
                'name': 'the_os_name',
                'version': 'the_os_version',
                'rooted': True,
                'build': 'the_os_build',
                'kernel_version': 'the_os_kernel_version',
            },
            'runtime': {
                'name': 'the_runtime_name',
                'version': 'the_runtime_version',
            },
            'browser': {
                'name': 'the_browser_name',
                'version': 'the_browser_version',
            },
            'device': {
                'model': 'the_device_model',
                'family': 'the_device_family',
                'name': 'the_device_name',
                'brand': 'the_device_brand',
                'locale': 'the_device_locale',
                'uuid': 'the_device_uuid',
                'model_id': 'the_device_model_id',
                'arch': 'the_device_arch',
                'battery_level': 30,
                'orientation': 'the_device_orientation',
                'simulator': False,
                'online': True,
                'charging': True,
            },
            'extra': {
                'type': 'extra',  # unnecessary
                'null': None,
                'int': 0,
                'float': 1.3,
                'list': [1, 2, 3],
                'dict': {'key': 'value'},
                'str': 'string',
            }
        }
        orig_tags = {
            'app.device': 'the_app_device_uuid',
            'os': 'the_os_name the_os_version',
            'os.name': 'the_os_name',
            'os.rooted': True,
            'runtime': 'the_runtime_name the_runtime_version',
            'runtime.name': 'the_runtime_name',
            'browser': 'the_browser_name the_browser_version',
            'browser.name': 'the_browser_name',
            'device': 'the_device_model',
            'device.family': 'the_device_family',
            'extra_tag': 'extra_value',
        }
        tags = orig_tags.copy()
        output = {}

        processor.extract_promoted_contexts(output, contexts, tags)

        assert output == {
            'app_device': u'the_app_device_uuid',
            'browser': u'the_browser_name the_browser_version',
            'browser_name': u'the_browser_name',
            'device': u'the_device_model',
            'device_arch': u'the_device_arch',
            'device_battery_level': 30.0,
            'device_brand': u'the_device_brand',
            'device_charging': True,
            'device_family': u'the_device_family',
            'device_locale': u'the_device_locale',
            'device_model_id': u'the_device_model_id',
            'device_name': u'the_device_name',
            'device_online': True,
            'device_orientation': u'the_device_orientation',
            'device_simulator': False,
            'device_uuid': u'the_device_uuid',
            'os': u'the_os_name the_os_version',
            'os_build': u'the_os_build',
            'os_kernel_version': u'the_os_kernel_version',
            'os_name': u'the_os_name',
            'os_rooted': True,
            'runtime': u'the_runtime_name the_runtime_version',
            'runtime_name': u'the_runtime_name',
        }
        assert contexts == {
            'app': {},
            'browser': {},
            'device': {},
            'extra': {
                'dict': {'key': 'value'},
                'float': 1.3,
                'int': 0,
                'list': [1, 2, 3],
                'null': None,
                'type': 'extra',
                'str': 'string',
            },
            'os': {},
            'runtime': {},
        }
        assert tags == orig_tags

        extra_output = {}
        processor.extract_extra_contexts(extra_output, contexts)

        assert extra_output == {
            'contexts.key': ['extra.int', 'extra.float', 'extra.str'],
            'contexts.value': [u'0', u'1.3', u'string'],
        }

    def test_extract_user(self):
        user = {
            'id': 'user_id',
            'email': 'user_email',
            'username': 'user_username',
            'ip_address': 'user_ip_address',
        }
        output = {}

        processor.extract_user(output, user)

        assert output == {'email': u'user_email',
                          'ip_address': u'user_ip_address',
                          'user_id': u'user_id',
                          'username': u'user_username'}

    def test_extract_geo(self):
        geo = {
            'country_code': 'US',
            'city': 'San Francisco',
            'region': 'CA',
        }
        output = {}

        processor.extract_geo(output, geo)

        assert output == {
            'geo_country_code': 'US',
            'geo_city': 'San Francisco',
            'geo_region': 'CA',
        }

    def test_extract_http(self):
        http = {
            'method': 'GET',
            'headers': [
                ['Referer', 'https://sentry.io'],
                ['Host', 'https://google.com'],
            ]
        }
        output = {}

        processor.extract_http(output, http)

        assert output == {'http_method': u'GET', 'http_referer': u'https://sentry.io'}

    def test_extract_stacktraces(self):
        stacks = [
            {'module': 'java.lang',
             'mechanism': {
                 'type': 'promise',
                 'description': 'globally unhandled promise rejection',
                 'help_link': 'http://example.com',
                 'handled': False,
                 'data': {
                     'polyfill': 'Bluebird'
                 },
                 'meta': {
                     'errno': {
                         'number': 123112,
                         'name': ''
                     }
                 }
             },
             'stacktrace': {
                 'frames': [
                     {'abs_path': 'Thread.java',
                      'filename': 'Thread.java',
                      'function': 'run',
                      'in_app': False,
                      'lineno': 748,
                      'module': 'java.lang.Thread'},
                     {'abs_path': 'ExecJavaMojo.java',
                      'filename': 'ExecJavaMojo.java',
                      'function': 'run',
                      'in_app': False,
                      'lineno': 293,
                      'module': 'org.codehaus.mojo.exec.ExecJavaMojo$1'},
                     {'abs_path': 'Method.java',
                      'filename': 'Method.java',
                      'function': 'invoke',
                      'in_app': False,
                      'colno': 19,
                      'lineno': 498,
                      'module': 'java.lang.reflect.Method'},
                     {'abs_path': 'DelegatingMethodAccessorImpl.java',
                      'filename': 'DelegatingMethodAccessorImpl.java',
                      'function': 'invoke',
                      'in_app': False,
                      'package': 'foo.bar',
                      'lineno': 43,
                      'module': 'sun.reflect.DelegatingMethodAccessorImpl'},
                     {'abs_path': 'NativeMethodAccessorImpl.java',
                      'filename': 'NativeMethodAccessorImpl.java',
                      'function': 'invoke',
                      'in_app': False,
                      'lineno': 62,
                      'module': 'sun.reflect.NativeMethodAccessorImpl'},
                     {'abs_path': 'NativeMethodAccessorImpl.java',
                      'filename': 'NativeMethodAccessorImpl.java',
                      'function': 'invoke0',
                      'in_app': False,
                      'module': 'sun.reflect.NativeMethodAccessorImpl'},
                     {'abs_path': 'Application.java',
                      'filename': 'Application.java',
                      'function': 'main',
                      'in_app': True,
                      'lineno': 17,
                      'module': 'io.sentry.example.Application'}]},
             'type': 'ArithmeticException',
             'value': '/ by zero'}]
        output = {}

        processor.extract_stacktraces(output, stacks)

        assert output == {
            'exception_frames.abs_path': [u'Thread.java',
                                          u'ExecJavaMojo.java',
                                          u'Method.java',
                                          u'DelegatingMethodAccessorImpl.java',
                                          u'NativeMethodAccessorImpl.java',
                                          u'NativeMethodAccessorImpl.java',
                                          u'Application.java'],
            'exception_frames.colno': [None, None, 19, None, None, None, None],
            'exception_frames.filename': [u'Thread.java',
                                          u'ExecJavaMojo.java',
                                          u'Method.java',
                                          u'DelegatingMethodAccessorImpl.java',
                                          u'NativeMethodAccessorImpl.java',
                                          u'NativeMethodAccessorImpl.java',
                                          u'Application.java'],
            'exception_frames.function': [u'run',
                                          u'run',
                                          u'invoke',
                                          u'invoke',
                                          u'invoke',
                                          u'invoke0',
                                          u'main'],
            'exception_frames.in_app': [False, False, False, False, False, False, True],
            'exception_frames.lineno': [748, 293, 498, 43, 62, None, 17],
            'exception_frames.module': [u'java.lang.Thread',
                                        u'org.codehaus.mojo.exec.ExecJavaMojo$1',
                                        u'java.lang.reflect.Method',
                                        u'sun.reflect.DelegatingMethodAccessorImpl',
                                        u'sun.reflect.NativeMethodAccessorImpl',
                                        u'sun.reflect.NativeMethodAccessorImpl',
                                        u'io.sentry.example.Application'],
            'exception_frames.package': [None, None, None, u'foo.bar', None, None, None],
            'exception_frames.stack_level': [0, 0, 0, 0, 0, 0, 0],
            'exception_stacks.type': [u'ArithmeticException'],
            'exception_stacks.value': [u'/ by zero'],
            'exception_stacks.mechanism_handled': [False],
            'exception_stacks.mechanism_type': [u'promise'],
        }
