import calendar
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Optional

import pytest

from snuba import settings
from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.events_format import (
    enforce_retention,
    extract_base,
    extract_extra_contexts,
    extract_extra_tags,
    extract_http,
    extract_user,
)
from snuba.datasets.events_processor_base import InsertEvent
from snuba.datasets.factory import enforce_table_writer
from snuba.processor import (
    InsertBatch,
    InvalidMessageType,
    InvalidMessageVersion,
    ProcessedMessage,
    ReplacementBatch,
)
from tests.base import BaseEventsTest


class TestEventsProcessor(BaseEventsTest):

    metadata = KafkaMessageMetadata(0, 0, datetime.now())

    def test_invalid_version(self) -> None:
        with pytest.raises(InvalidMessageVersion):
            enforce_table_writer(
                self.dataset
            ).get_stream_loader().get_processor().process_message(
                (2 ** 32 - 1, "insert", self.event), self.metadata,
            )

    def test_invalid_format(self) -> None:
        with pytest.raises(InvalidMessageVersion):
            enforce_table_writer(
                self.dataset
            ).get_stream_loader().get_processor().process_message(
                (-1, "insert", self.event), self.metadata,
            )

    def __process_insert_event(self, event: InsertEvent) -> Optional[ProcessedMessage]:
        return (
            enforce_table_writer(self.dataset)
            .get_stream_loader()
            .get_processor()
            .process_message((2, "insert", event, {}), self.metadata)
        )

    def test_unexpected_obj(self) -> None:
        self.event["message"] = {"what": "why is this in the message"}

        processed = self.__process_insert_event(self.event)
        assert isinstance(processed, InsertBatch)
        assert processed.rows[0]["message"] == '{"what": "why is this in the message"}'

    def test_hash_invalid_primary_hash(self) -> None:
        self.event[
            "primary_hash"
        ] = b"'tinymce' \u063a\u064a\u0631 \u0645\u062d".decode("unicode-escape")

        processed = self.__process_insert_event(self.event)
        assert isinstance(processed, InsertBatch)
        assert processed.rows[0]["primary_hash"] == "a52ccc1a61c2258e918b43b5aff50db1"

    def test_extract_required(self):
        now = datetime.utcnow()
        event = {
            "event_id": "1" * 32,
            "project_id": 100,
            "group_id": 10,
            "datetime": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        output = {}
        extract_base(output, event)
        output["retention_days"] = enforce_retention(
            event,
            datetime.strptime(event["datetime"], settings.PAYLOAD_DATETIME_FORMAT),
        )
        enforce_table_writer(
            self.dataset
        ).get_stream_loader().get_processor().extract_required(output, event)
        assert output == {
            "event_id": "11111111111111111111111111111111",
            "project_id": 100,
            "group_id": 10,
            "timestamp": now,
            "retention_days": settings.DEFAULT_RETENTION_DAYS,
        }

    def test_extract_common(self):
        now = datetime.utcnow().replace(microsecond=0)
        event = {
            "primary_hash": "a" * 32,
            "message": "the message",
            "platform": "the_platform",
            "data": {
                "received": int(calendar.timegm(now.timetuple())),
                "culprit": "the culprit",
                "type": "error",
                "version": 6,
                "title": "FooError",
                "location": "bar.py",
                "modules": OrderedDict([("foo", "1.0"), ("bar", "2.0"), ("baz", None)]),
            },
        }
        output = {}

        enforce_table_writer(
            self.dataset
        ).get_stream_loader().get_processor().extract_common(
            output, event, self.metadata
        )
        assert output == {
            "platform": u"the_platform",
            "primary_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "received": now,
            "culprit": "the culprit",
            "type": "error",
            "version": "6",
            "modules.name": [u"foo", u"bar", u"baz"],
            "modules.version": [u"1.0", u"2.0", u""],
            "title": "FooError",
            "location": "bar.py",
        }

    def test_v2_invalid_type(self):
        with pytest.raises(InvalidMessageType):
            assert (
                enforce_table_writer(self.dataset)
                .get_stream_loader()
                .get_processor()
                .process_message((2, "__invalid__", {}), self.metadata)
                == 1
            )

    def test_v2_start_delete_groups(self):
        project_id = 1
        message = (2, "start_delete_groups", {"project_id": project_id})
        processor = (
            enforce_table_writer(self.dataset).get_stream_loader().get_processor()
        )
        assert processor.process_message(message, self.metadata) == ReplacementBatch(
            str(project_id), [message]
        )

    def test_v2_end_delete_groups(self):
        project_id = 1
        message = (2, "end_delete_groups", {"project_id": project_id})
        processor = (
            enforce_table_writer(self.dataset).get_stream_loader().get_processor()
        )
        assert processor.process_message(message, self.metadata) == ReplacementBatch(
            str(project_id), [message]
        )

    def test_v2_start_merge(self):
        project_id = 1
        message = (2, "start_merge", {"project_id": project_id})
        processor = (
            enforce_table_writer(self.dataset).get_stream_loader().get_processor()
        )
        assert processor.process_message(message, self.metadata) == ReplacementBatch(
            str(project_id), [message]
        )

    def test_v2_end_merge(self):
        project_id = 1
        message = (2, "end_merge", {"project_id": project_id})
        processor = (
            enforce_table_writer(self.dataset).get_stream_loader().get_processor()
        )
        assert processor.process_message(message, self.metadata) == ReplacementBatch(
            str(project_id), [message]
        )

    def test_v2_start_unmerge(self):
        project_id = 1
        message = (2, "start_unmerge", {"project_id": project_id})
        processor = (
            enforce_table_writer(self.dataset).get_stream_loader().get_processor()
        )
        assert processor.process_message(message, self.metadata) == ReplacementBatch(
            str(project_id), [message]
        )

    def test_v2_end_unmerge(self):
        project_id = 1
        message = (2, "end_unmerge", {"project_id": project_id})
        processor = (
            enforce_table_writer(self.dataset).get_stream_loader().get_processor()
        )
        assert processor.process_message(message, self.metadata) == ReplacementBatch(
            str(project_id), [message]
        )

    def test_v2_start_delete_tag(self):
        project_id = 1
        message = (2, "start_delete_tag", {"project_id": project_id})
        processor = (
            enforce_table_writer(self.dataset).get_stream_loader().get_processor()
        )
        assert processor.process_message(message, self.metadata) == ReplacementBatch(
            str(project_id), [message]
        )

    def test_v2_end_delete_tag(self):
        project_id = 1
        message = (2, "end_delete_tag", {"project_id": project_id})
        processor = (
            enforce_table_writer(self.dataset).get_stream_loader().get_processor()
        )
        assert processor.process_message(message, self.metadata) == ReplacementBatch(
            str(project_id), [message]
        )

    def test_extract_sdk(self):
        sdk = {
            "integrations": ["logback"],
            "name": "sentry-java",
            "version": "1.6.1-d1e3a",
        }
        output = {}

        enforce_table_writer(
            self.dataset
        ).get_stream_loader().get_processor().extract_sdk(output, sdk)

        assert output == {
            "sdk_name": u"sentry-java",
            "sdk_version": u"1.6.1-d1e3a",
            "sdk_integrations": [u"logback"],
        }

    def test_extract_tags(self):
        orig_tags = {
            "sentry:user": "the_user",
            "level": "the_level",
            "logger": "the_logger",
            "server_name": "the_servername",
            "transaction": "the_transaction",
            "environment": "the_enviroment",
            "sentry:release": "the_release",
            "sentry:dist": "the_dist",
            "site": "the_site",
            "url": "the_url",
            "extra_tag": "extra_value",
            "null_tag": None,
        }
        tags = orig_tags.copy()
        output = {}

        enforce_table_writer(
            self.dataset
        ).get_stream_loader().get_processor().extract_promoted_tags(output, tags)

        assert output == {
            "sentry:dist": "the_dist",
            "environment": u"the_enviroment",
            "level": u"the_level",
            "logger": u"the_logger",
            "sentry:release": "the_release",
            "server_name": u"the_servername",
            "site": u"the_site",
            "transaction": u"the_transaction",
            "url": u"the_url",
            "sentry:user": u"the_user",
        }
        assert tags == orig_tags

        extra_output = {}
        extra_output["tags.key"], extra_output["tags.value"] = extract_extra_tags(tags)

        valid_items = [(k, v) for k, v in sorted(orig_tags.items()) if v]
        assert extra_output == {
            "tags.key": [k for k, v in valid_items],
            "tags.value": [v for k, v in valid_items],
        }

    def test_extract_tags_empty_string(self):
        # verify our text field extraction doesn't coerce '' to None
        tags = {
            "environment": "",
        }
        output = {}

        enforce_table_writer(
            self.dataset
        ).get_stream_loader().get_processor().extract_promoted_tags(output, tags)

        assert output["environment"] == u""

    def test_extract_contexts(self):
        contexts = {
            "app": {"device_app_hash": "the_app_device_uuid"},
            "os": {
                "name": "the_os_name",
                "version": "the_os_version",
                "rooted": True,
                "build": "the_os_build",
                "kernel_version": "the_os_kernel_version",
            },
            "runtime": {"name": "the_runtime_name", "version": "the_runtime_version"},
            "browser": {"name": "the_browser_name", "version": "the_browser_version"},
            "device": {
                "model": "the_device_model",
                "family": "the_device_family",
                "name": "the_device_name",
                "brand": "the_device_brand",
                "locale": "the_device_locale",
                "uuid": "the_device_uuid",
                "model_id": "the_device_model_id",
                "arch": "the_device_arch",
                "battery_level": 30,
                "orientation": "the_device_orientation",
                "simulator": False,
                "online": True,
                "charging": True,
            },
            "extra": {
                "type": "extra",  # unnecessary
                "null": None,
                "int": 0,
                "float": 1.3,
                "list": [1, 2, 3],
                "dict": {"key": "value"},
                "str": "string",
                "\ud83c": "invalid utf-8 surrogate",
            },
        }
        orig_tags = {
            "app.device": "the_app_device_uuid",
            "os": "the_os_name the_os_version",
            "os.name": "the_os_name",
            "os.rooted": True,
            "runtime": "the_runtime_name the_runtime_version",
            "runtime.name": "the_runtime_name",
            "browser": "the_browser_name the_browser_version",
            "browser.name": "the_browser_name",
            "device": "the_device_model",
            "device.family": "the_device_family",
            "extra_tag": "extra_value",
        }
        tags = orig_tags.copy()
        output = {}

        enforce_table_writer(
            self.dataset
        ).get_stream_loader().get_processor().extract_promoted_contexts(
            output, contexts, tags
        )

        assert output == {
            "app_device": u"the_app_device_uuid",
            "browser": u"the_browser_name the_browser_version",
            "browser_name": u"the_browser_name",
            "device": u"the_device_model",
            "device_arch": u"the_device_arch",
            "device_battery_level": 30.0,
            "device_brand": u"the_device_brand",
            "device_charging": True,
            "device_family": u"the_device_family",
            "device_locale": u"the_device_locale",
            "device_model_id": u"the_device_model_id",
            "device_name": u"the_device_name",
            "device_online": True,
            "device_orientation": u"the_device_orientation",
            "device_simulator": False,
            "device_uuid": u"the_device_uuid",
            "os": u"the_os_name the_os_version",
            "os_build": u"the_os_build",
            "os_kernel_version": u"the_os_kernel_version",
            "os_name": u"the_os_name",
            "os_rooted": True,
            "runtime": u"the_runtime_name the_runtime_version",
            "runtime_name": u"the_runtime_name",
        }
        assert contexts == {
            "app": {},
            "browser": {},
            "device": {},
            "extra": {
                "dict": {"key": "value"},
                "\ud83c": "invalid utf-8 surrogate",
                "float": 1.3,
                "int": 0,
                "list": [1, 2, 3],
                "null": None,
                "type": "extra",
                "str": "string",
            },
            "os": {},
            "runtime": {},
        }
        assert tags == orig_tags

        extra_output = {}
        (
            extra_output["contexts.key"],
            extra_output["contexts.value"],
        ) = extract_extra_contexts(contexts)

        assert extra_output == {
            "contexts.key": ["extra.int", "extra.float", "extra.str", "extra.\\ud83c"],
            "contexts.value": [u"0", u"1.3", u"string", u"invalid utf-8 surrogate"],
        }

    def test_extract_user(self):
        user = {
            "id": "user_id",
            "email": "user_email",
            "username": "user_username",
            "ip_address": "127.0.0.2",
        }
        output = {}

        extract_user(output, user)

        assert output == {
            "email": u"user_email",
            "ip_address": u"127.0.0.2",
            "user_id": u"user_id",
            "username": u"user_username",
        }

    def test_extract_geo(self):
        geo = {
            "country_code": "US",
            "city": "San Francisco",
            "region": "CA",
        }
        output = {}

        enforce_table_writer(
            self.dataset
        ).get_stream_loader().get_processor().extract_geo(output, geo)

        assert output == {
            "geo_country_code": "US",
            "geo_city": "San Francisco",
            "geo_region": "CA",
        }

    def test_extract_http(self):
        request = {
            "method": "GET",
            "headers": [
                ["Referer", "https://sentry.io"],
                ["Host", "https://google.com"],
            ],
            "url": "the_url",
        }
        output = {}

        extract_http(output, request)

        assert output == {
            "http_method": u"GET",
            "http_referer": u"https://sentry.io",
            "http_url": "the_url",
        }

    def test_extract_stacktraces(self):
        stacks = [
            {
                "module": "java.lang",
                "mechanism": {
                    "type": "promise",
                    "description": "globally unhandled promise rejection",
                    "help_link": "http://example.com",
                    "handled": False,
                    "data": {"polyfill": "Bluebird"},
                    "meta": {"errno": {"number": 123112, "name": ""}},
                },
                "stacktrace": {
                    "frames": [
                        {
                            "abs_path": "Thread.java",
                            "filename": "Thread.java",
                            "function": "run",
                            "in_app": False,
                            "lineno": 748,
                            "module": "java.lang.Thread",
                        },
                        {
                            "abs_path": "ExecJavaMojo.java",
                            "filename": "ExecJavaMojo.java",
                            "function": "run",
                            "in_app": False,
                            "lineno": 293,
                            "module": "org.codehaus.mojo.exec.ExecJavaMojo$1",
                        },
                        {
                            "abs_path": "Method.java",
                            "filename": "Method.java",
                            "function": "invoke",
                            "in_app": False,
                            "colno": 19,
                            "lineno": 498,
                            "module": "java.lang.reflect.Method",
                        },
                        {
                            "abs_path": "DelegatingMethodAccessorImpl.java",
                            "filename": "DelegatingMethodAccessorImpl.java",
                            "function": "invoke",
                            "in_app": False,
                            "package": "foo.bar",
                            "lineno": 43,
                            "module": "sun.reflect.DelegatingMethodAccessorImpl",
                        },
                        {
                            "abs_path": "NativeMethodAccessorImpl.java",
                            "filename": "NativeMethodAccessorImpl.java",
                            "function": "invoke",
                            "in_app": False,
                            "lineno": 62,
                            "module": "sun.reflect.NativeMethodAccessorImpl",
                        },
                        {
                            "abs_path": "NativeMethodAccessorImpl.java",
                            "filename": "NativeMethodAccessorImpl.java",
                            "function": "invoke0",
                            "in_app": False,
                            "module": "sun.reflect.NativeMethodAccessorImpl",
                        },
                        {
                            "abs_path": "Application.java",
                            "filename": "Application.java",
                            "function": "main",
                            "in_app": True,
                            "lineno": 17,
                            "module": "io.sentry.example.Application",
                        },
                    ]
                },
                "type": "ArithmeticException",
                "value": "/ by zero",
            }
        ]

        output = {"project_id": 1}

        enforce_table_writer(
            self.dataset
        ).get_stream_loader().get_processor().extract_stacktraces(output, stacks)

        assert output == {
            "project_id": 1,
            "exception_frames.abs_path": [
                u"Thread.java",
                u"ExecJavaMojo.java",
                u"Method.java",
                u"DelegatingMethodAccessorImpl.java",
                u"NativeMethodAccessorImpl.java",
                u"NativeMethodAccessorImpl.java",
                u"Application.java",
            ],
            "exception_frames.colno": [None, None, 19, None, None, None, None],
            "exception_frames.filename": [
                u"Thread.java",
                u"ExecJavaMojo.java",
                u"Method.java",
                u"DelegatingMethodAccessorImpl.java",
                u"NativeMethodAccessorImpl.java",
                u"NativeMethodAccessorImpl.java",
                u"Application.java",
            ],
            "exception_frames.function": [
                u"run",
                u"run",
                u"invoke",
                u"invoke",
                u"invoke",
                u"invoke0",
                u"main",
            ],
            "exception_frames.in_app": [False, False, False, False, False, False, True],
            "exception_frames.lineno": [748, 293, 498, 43, 62, None, 17],
            "exception_frames.module": [
                u"java.lang.Thread",
                u"org.codehaus.mojo.exec.ExecJavaMojo$1",
                u"java.lang.reflect.Method",
                u"sun.reflect.DelegatingMethodAccessorImpl",
                u"sun.reflect.NativeMethodAccessorImpl",
                u"sun.reflect.NativeMethodAccessorImpl",
                u"io.sentry.example.Application",
            ],
            "exception_frames.package": [
                None,
                None,
                None,
                u"foo.bar",
                None,
                None,
                None,
            ],
            "exception_frames.stack_level": [0, 0, 0, 0, 0, 0, 0],
            "exception_stacks.type": [u"ArithmeticException"],
            "exception_stacks.value": [u"/ by zero"],
            "exception_stacks.mechanism_handled": [False],
            "exception_stacks.mechanism_type": [u"promise"],
        }

    def test_null_values_dont_throw(self) -> None:
        event = {
            "event_id": "bce76c2473324fa387b33564eacf34a0",
            "group_id": 1,
            "primary_hash": "a" * 32,
            "project_id": 70156,
            "message": None,
            "platform": None,
            "data": {
                "culprit": None,
                "errors": None,
                "extra": None,
                "fingerprint": None,
                "http": None,
                "id": "bce76c2473324fa387b33564eacf34a0",
                "message": None,
                "metadata": None,
                "platform": None,
                "project": 70156,
                "release": None,
                "dist": None,
                "sdk": None,
                "sentry.interfaces.Exception": {"exc_omitted": None, "values": None},
                "sentry.interfaces.Message": None,
                "tags": None,
                "time_spent": None,
                "type": None,
                "version": None,
            },
        }

        timestamp = datetime.utcnow()
        event["datetime"] = (timestamp - timedelta(seconds=2)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        event["data"]["received"] = int(
            calendar.timegm((timestamp - timedelta(seconds=1)).timetuple())
        )

        self.__process_insert_event(event)
