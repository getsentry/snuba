import calendar
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from snuba import settings


def get_raw_event() -> Any:
    now = datetime.utcnow()

    return {
        "datetime": (now - timedelta(seconds=2)).strftime(
            settings.PAYLOAD_DATETIME_FORMAT,
        ),
        "received": int(calendar.timegm((now - timedelta(seconds=1)).timetuple())),
        "culprit": "io.sentry.example.Application in main",
        "organization_id": 3,
        "errors": [],
        "title": "Exception!",
        "extra": {"Sentry-Threadname": "io.sentry.example.Application.main()"},
        "fingerprint": ["{{ default }}"],
        "id": "bce76c2473324fa387b33564eacf34a0",
        "key_id": 113866,
        "message": "Caught exception!",
        "metadata": {"type": "ArithmeticException", "value": "/ by zero"},
        "platform": "java",
        "project": 70156,
        "release": "1.0",
        "dist": None,
        "sdk": {
            "integrations": ["logback"],
            "name": "sentry-java",
            "version": "1.6.1-d1e3a",
        },
        "request": {
            "url": "http://127.0.0.1:/query",
            "headers": [
                ["Accept-Encoding", "identity"],
                ["Content-Length", "398"],
                ["Host", "127.0.0.1:"],
                ["Referer", "tagstore.something"],
                ["Trace", "8fa73032d-1"],
            ],
            "data": "",
            "method": "POST",
            "env": {"SERVER_PORT": "1010", "SERVER_NAME": "snuba"},
        },
        "contexts": {
            "device": {"online": True, "charging": True, "model_id": "Galaxy"}
        },
        "sentry.interfaces.Exception": {
            "exc_omitted": None,
            "values": [
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
                                "lineno": 498,
                                "module": "java.lang.reflect.Method",
                            },
                            {
                                "abs_path": "DelegatingMethodAccessorImpl.java",
                                "filename": "DelegatingMethodAccessorImpl.java",
                                "function": "invoke",
                                "in_app": False,
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
            ],
        },
        "sentry.interfaces.Message": {"message": "Caught exception!"},
        "tags": [
            ["logger", "example.Application"],
            ["server_name", "localhost.localdomain"],
            ["level", "error"],
            ["custom_tag", "custom_value"],
            ["url", "http://127.0.0.1:/query"],
        ],
        "time_spent": None,
        "type": "error",
        "version": "6",
    }


def get_raw_transaction() -> Any:
    now = datetime.utcnow().replace(
        minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    start_timestamp = now - timedelta(seconds=3)
    end_timestamp = now - timedelta(seconds=2)
    event_received = now - timedelta(seconds=1)
    trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
    span_id = "8841662216cc598b"

    return {
        "project_id": 70156,
        "event_id": uuid.uuid4().hex,
        "deleted": 0,
        "datetime": end_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "platform": "python",
        "retention_days": settings.DEFAULT_RETENTION_DAYS,
        "data": {
            "received": calendar.timegm(event_received.timetuple()),
            "type": "transaction",
            "transaction": "/api/do_things",
            "start_timestamp": datetime.timestamp(start_timestamp),
            "timestamp": datetime.timestamp(end_timestamp),
            "tags": {
                # Sentry
                "environment": u"prød",
                "sentry:release": "1",
                "sentry:dist": "dist1",
                "url": "http://127.0.0.1:/query",
                # User
                "foo": "baz",
                "foo.bar": "qux",
                "os_name": "linux",
            },
            "user": {
                "email": "sally@example.org",
                "ip_address": "8.8.8.8",
                "geo": {"city": "San Francisco", "region": "CA", "country_code": "US"},
            },
            "contexts": {
                "trace": {"trace_id": trace_id.hex, "span_id": span_id, "op": "http"},
                "device": {"online": True, "charging": True, "model_id": "Galaxy"},
            },
            "measurements": {
                "lcp": {"value": 32.129},
                "lcp.elementSize": {"value": 4242},
            },
            "sdk": {
                "name": "sentry.python",
                "version": "0.13.4",
                "integrations": ["django"],
            },
            "request": {
                "url": "http://127.0.0.1:/query",
                "headers": [
                    ["Accept-Encoding", "identity"],
                    ["Content-Length", "398"],
                    ["Host", "127.0.0.1:"],
                    ["Referer", "tagstore.something"],
                    ["Trace", "8fa73032d-1"],
                ],
                "data": "",
                "method": "POST",
                "env": {"SERVER_PORT": "1010", "SERVER_NAME": "snuba"},
            },
            "spans": [
                {
                    "op": "db",
                    "trace_id": trace_id.hex,
                    "span_id": span_id + "1",
                    "parent_span_id": None,
                    "same_process_as_parent": True,
                    "description": "SELECT * FROM users",
                    "data": {},
                    "timestamp": calendar.timegm(end_timestamp.timetuple()),
                }
            ],
        },
    }
