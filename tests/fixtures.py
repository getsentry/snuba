from __future__ import annotations

import calendar
import json
import uuid
from datetime import datetime, timedelta, timezone
from hashlib import md5
from typing import Any, Mapping, Tuple

from snuba import settings
from snuba.processor import InsertEvent

PROJECT_ID = 70156
ORG_ID = 1123


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
    trace_id = str(uuid.uuid4().hex)
    span_id = "deadbeef"

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
                "os": {
                    "kernel_version": "1.1.1",
                    "name": "android",
                    "version": "1.1.1",
                },
                "trace": {"trace_id": trace_id, "span_id": span_id},
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
                        "thread_id": 1,
                    }
                ],
            },
            "sentry.interfaces.Threads": {
                "values": [
                    {
                        "id": 1,
                        "main": True,
                    },
                ]
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


def get_raw_transaction(span_id: str | None = None) -> Mapping[str, Any]:
    now = datetime.utcnow().replace(
        minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    start_timestamp = now - timedelta(seconds=3)
    end_timestamp = now - timedelta(seconds=2)
    event_received = now - timedelta(seconds=1)
    trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
    span_id = "8841662216cc598b" if not span_id else span_id
    unique = "100"
    primary_hash = md5(unique.encode("utf-8")).hexdigest()
    app_start_type = "warm.prewarmed"
    profile_id = uuid.UUID("046852d2-4483-455c-8c44-f0c8fbf496f9")

    return {
        "project_id": PROJECT_ID,
        "event_id": uuid.uuid4().hex,
        "deleted": 0,
        "datetime": end_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "platform": "python",
        "retention_days": settings.DEFAULT_RETENTION_DAYS,
        "group_ids": [int(primary_hash[:16], 16)],
        "data": {
            "received": calendar.timegm(event_received.timetuple()),
            "type": "transaction",
            "transaction": "/api/do_things",
            "start_timestamp": datetime.timestamp(start_timestamp),
            "timestamp": datetime.timestamp(end_timestamp),
            "tags": {
                # Sentry
                "environment": "prød",
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
                "trace": {
                    "trace_id": trace_id.hex,
                    "span_id": span_id,
                    "op": "http",
                    "hash": "05029609156d8133",
                    "exclusive_time": 1.2,
                },
                "device": {"online": True, "charging": True, "model_id": "Galaxy"},
                "app": {"start_type": app_start_type},
                "profile": {"profile_id": profile_id.hex},
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
                    "hash": "05029609156d8133",
                    "exclusive_time": 1.2,
                }
            ],
        },
    }


def get_replay_event(replay_id: str | None = None) -> Mapping[str, Any]:
    replay_id = (
        replay_id if replay_id else str(uuid.UUID("e5e062bf2e1d4afd96fd2f90b6770431"))
    )
    now = (
        datetime.utcnow()
        .replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        .timestamp()
    )

    return {
        "type": "replay_event",
        "start_time": now,
        "replay_id": replay_id,
        "project_id": 1,
        "retention_days": 30,
        "payload": list(
            bytes(
                json.dumps(
                    {
                        "type": "replay_event",
                        "replay_id": replay_id,
                        "segment_id": 0,
                        "tags": {
                            "customtag": "is_set",
                            "transaction": "/organizations/:orgId/issues/",
                        },
                        "trace_ids": [
                            "36e980a9-c602-4cde-9f5d-089f15b83b5f",
                            "8bea4461-d8b9-44f3-93c1-5a3cb1c4169a",
                        ],
                        "dist": "",
                        "platform": "python",
                        "timestamp": now,
                        "environment": "prod",
                        "release": "34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a",
                        "user": {
                            "id": "232",
                            "username": "me",
                            "email": "test@test.com",
                            "ip_address": "127.0.0.1",
                        },
                        "sdk": {
                            "name": "sentry.python",
                            "version": "7",
                        },
                        "contexts": {
                            "trace": {
                                "op": "pageload",
                                "span_id": "affa5649681a1eeb",
                                "trace_id": "23eda6cd4b174ef8a51f0096df3bfdd1",
                            }
                        },
                        "request": {
                            "url": "http://127.0.0.1:3000/",
                            "headers": {
                                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
                            },
                        },
                        "extra": {},
                    },
                ).encode()
            )
        ),
    }


def get_raw_error_message() -> Tuple[int, str, InsertEvent]:
    """
    Get an error message which can be passed to the processors.
    """
    return (
        2,
        "insert",
        get_raw_event(),
    )


def get_raw_transaction_message() -> Tuple[int, str, Mapping[str, Any]]:
    """
    Get a transaction message which can be passed to the processors.
    """
    return (
        2,
        "insert",
        get_raw_transaction(),
    )
