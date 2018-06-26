import calendar
import pytest
import simplejson as json
from datetime import datetime, timedelta

from base import BaseTest

from snuba import processor, settings
from snuba.processor import get_key, process_message, ProcessorWorker


class TestProcessor(BaseTest):
    def test_key(self):
        key = get_key(self.event)

        assert self.event['event_id'] in key
        assert str(self.event['project_id']) in key

    def test_simple(self):
        _, _, processed = process_message(self.event)

        for field in ('event_id', 'project_id', 'message', 'platform'):
            assert processed[field] == self.event[field]
        assert isinstance(processed['timestamp'], int)
        assert isinstance(processed['received'], int)

    def test_simple_version_0(self):
        _, _, processed = process_message((0, 'insert', self.event))

        for field in ('event_id', 'project_id', 'message', 'platform'):
            assert processed[field] == self.event[field]
        assert isinstance(processed['timestamp'], int)
        assert isinstance(processed['received'], int)

    def test_invalid_action_version_0(self):
        with pytest.raises(ValueError):
            process_message((1, 'invalid', self.event))

    def test_invalid_format(self):
        with pytest.raises(ValueError):
            process_message((-1, 'insert', self.event))

    def test_unexpected_obj(self):
        self.event['message'] = {'what': 'why is this in the message'}

        _, _, processed = process_message(self.event)

        assert processed['message'] == '{"what": "why is this in the message"}'

    def test_hash_invalid_primary_hash(self):
        self.event['primary_hash'] = b"'tinymce' \u063a\u064a\u0631 \u0645\u062d".decode('unicode-escape')

        _, _, processed = process_message(self.event)

        assert processed['primary_hash'] == 'a52ccc1a61c2258e918b43b5aff50db1'

    def test_extract_required(self):
        now = datetime.utcnow()
        event = {
            'event_id': '1' * 32,
            'project_id': 100,
            'datetime': now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        output = {}

        processor.extract_required(output, event)
        assert output == {
            'event_id': '11111111111111111111111111111111',
            'project_id': 100,
            'timestamp': int(calendar.timegm(now.timetuple())),
            'retention_days': settings.DEFAULT_RETENTION_DAYS,
        }

    def test_extract_common(self):
        event = {
            'primary_hash': 'a' * 32,
            'message': 'the message',
            'platform': 'the_platform',
        }
        data = {
            'received': 1520971716.0,
            'type': 'error',
            'version': 6,
        }
        output = {}

        processor.extract_common(output, event, data)
        assert output == {
            'message': u'the message',
            'platform': u'the_platform',
            'primary_hash': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'received': 1520971716,
            'type': 'error',
            'version': '6',
        }

    def test_deleted(self):
        now = datetime.utcnow()
        message = (0, 'delete', {
            'event_id': '1' * 32,
            'project_id': 100,
            'datetime': now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'deleted': True,
        })

        _, _, processed = processor.process_message(message)
        assert processed == {
            'event_id': '11111111111111111111111111111111',
            'project_id': 100,
            'timestamp': int(calendar.timegm(now.timetuple())),
            'deleted': True,
            'retention_days': settings.DEFAULT_RETENTION_DAYS,
        }

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
            'exception_frames.lineno': [None, None, None, 43, 62, None, 17],
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

    def test_offsets(self):
        event = self.event

        class FakeMessage(object):
            def value(self):
                # event doesn't really matter
                return json.dumps((0, 'insert', event))

            def offset(self):
                return 123

            def partition(self):
                return 456

        test_worker = ProcessorWorker(producer=None, events_topic=None, deletes_topic=None)
        _, _, val = test_worker.process_message(FakeMessage())

        val = json.loads(val)

        assert val['project_id'] == self.event['project_id']
        assert val['event_id'] == self.event['event_id']
        assert val['offset'] == 123
        assert val['partition'] == 456

    def test_skip_too_old(self):
        test_worker = ProcessorWorker(producer=None, events_topic=None, deletes_topic=None)

        event = self.event
        old_timestamp = datetime.utcnow() - timedelta(days=300)
        old_timestamp_str = old_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        event['datetime'] = old_timestamp_str
        event['data']['datetime'] = old_timestamp_str
        event['data']['received'] = int(calendar.timegm(old_timestamp.timetuple()))

        class FakeMessage(object):
            def value(self):
                return json.dumps((0, 'insert', event))

        assert test_worker.process_message(FakeMessage()) is None
