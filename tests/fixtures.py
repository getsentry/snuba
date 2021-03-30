import calendar
import uuid
from datetime import datetime, timedelta, timezone
from hashlib import md5
from snuba import settings
from snuba.datasets.events_processor_base import InsertEvent

PROJECT_ID = 70156


def get_raw_event() -> InsertEvent:
    now = datetime.utcnow()

    event_id = str(uuid.uuid4().hex)
    message = "Caught exception!"
    unique = "%s:%s" % (str(PROJECT_ID), event_id)
    primary_hash = md5(unique.encode("utf-8")).hexdigest()
    platform = "java"
    event_datetime = (now - timedelta(seconds=2)).strftime(
        settings.PAYLOAD_DATETIME_FORMAT,
    )

    return {
        "project_id": PROJECT_ID,
        "event_id": event_id,
        "group_id": int(primary_hash[:16], 16),
        "primary_hash": primary_hash,
        "message": message,
        "platform": platform,
        "datetime": event_datetime,
        "organization_id": 3,
        "retention_days": settings.DEFAULT_RETENTION_DAYS,
        "data": {
            "datetime": event_datetime,
            "received": int(calendar.timegm((now - timedelta(seconds=1)).timetuple())),
            "culprit": "io.sentry.example.Application in main",
            "errors": [],
            "title": "Exception!",
            "extra": {"Sentry-Threadname": "io.sentry.example.Application.main()"},
            "fingerprint": ["{{ default }}"],
            "id": event_id,
            "key_id": 113866,
            "message": message,
            "metadata": {"type": "ArithmeticException", "value": "/ by zero"},
            "platform": platform,
            "project": PROJECT_ID,
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
            "user": {
                "email": "sally@example.org",
                "ip_address": "8.8.8.8",
                "geo": {"city": "San Francisco", "region": "CA", "country_code": "US"},
            },
            "contexts": {
                "device": {"online": True, "charging": True, "model_id": "Galaxy"},
                "os": {"kernel_version": "1.1.1"},
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
        },
    }


def get_raw_transaction() -> InsertEvent:
    now = datetime.utcnow().replace(
        minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    start_timestamp = now - timedelta(seconds=3)
    end_timestamp = now - timedelta(seconds=2)
    event_received = now - timedelta(seconds=1)
    trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
    span_id = "8841662216cc598b"

    return {
        "project_id": PROJECT_ID,
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
            "breakdowns": {
                "span_ops": {
                    "ops.db": {"value": 62.512},
                    "ops.http": {"value": 109.774},
                    "total.time": {"value": 172.286},
                }
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
