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
    profiler_id = uuid.UUID("ab90d5a8-0391-4b35-bd78-69d8f8e57469")

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
                "profile": {
                    "profile_id": profile_id.hex,
                    "profiler_id": profiler_id.hex,
                },
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
                        "tags": [
                            ["customtag", "is_set"],
                            ["transaction", "/organizations/:orgId/issues/"],
                        ],
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


def get_raw_error_message() -> Tuple[int, str, InsertEvent, Any]:
    """
    Get an error message which can be passed to the processors.
    """
    return (2, "insert", get_raw_event(), {})


def get_raw_transaction_message() -> Tuple[int, str, Mapping[str, Any]]:
    """
    Get a transaction message which can be passed to the processors.
    """
    return (2, "insert", get_raw_transaction())


def get_raw_join_query_trace() -> str:
    return """
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> executeQuery: (from 10.2.7.138:38110, user: someuser) SELECT t.thecount, e.thecount FROM ( SELECT trace_id, count() as thecount FROM errors_dist WHERE timestamp >= '2024-08-19T10:00:00' AND timestamp <= '2024-08-19T12:00:00' AND project_id = 2 AND trace_id = 'b18bc43f15e2482e9b7024a5750b502b' GROUP BY trace_id ) AS e JOIN ( SELECT trace_id, count() as thecount FROM transactions_dist WHERE finish_ts >= '2024-08-19T10:00:00' AND finish_ts <= '2024-08-19T12:00:00' AND project_id = 2 AND trace_id = 'b18bc43f15e2482e9b7024a5750b502b' GROUP BY trace_id ) AS t ON e.trace_id = t.trace_id (stage: Complete)
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> ContextAccess (someuser): Access granted: SELECT(project_id, timestamp, trace_id) ON default.errors_dist
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> ContextAccess (someuser): Access granted: SELECT(project_id, trace_id, finish_ts) ON default.transactions_dist
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> ContextAccess (someuser): Access granted: SELECT(project_id, timestamp, trace_id) ON default.errors_dist
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> ContextAccess (someuser): Access granted: SELECT(project_id, trace_id, finish_ts) ON default.transactions_dist
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> ContextAccess (someuser): Access granted: SELECT(project_id, trace_id, finish_ts) ON default.transactions_dist
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> InterpreterSelectQuery: Complete -> Complete
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> HashJoin: (0x7f3cbef9ec18) Datatype: EMPTY, kind: Inner, strictness: All, right header: t.trace_id Nullable(UUID) Nullable(size = 0, UUID(size = 0), UInt8(size = 0)), t.thecount UInt64 UInt64(size = 0)
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> HashJoin: (0x7f3cbef9ec18) Keys: [(trace_id) = (t.trace_id)]
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> ContextAccess (someuser): Access granted: SELECT(project_id, timestamp, trace_id) ON default.errors_dist
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> ContextAccess (someuser): Access granted: SELECT(project_id, timestamp, trace_id) ON default.errors_dist
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> InterpreterSelectQuery: Complete -> Complete
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> InterpreterSelectQuery: FetchColumns -> Complete
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> JoiningTransform: Before join block: 'trace_id Nullable(UUID) Nullable(size = 0, UUID(size = 0), UInt8(size = 0)), thecount UInt64 UInt64(size = 0)'
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> JoiningTransform: After join block: 'trace_id Nullable(UUID) Nullable(size = 0, UUID(size = 0), UInt8(size = 0)), thecount UInt64 UInt64(size = 0), t.thecount UInt64 UInt64(size = 0)'
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> JoiningTransform: Before join block: 'trace_id Nullable(UUID) Nullable(size = 0, UUID(size = 0), UInt8(size = 0)), thecount UInt64 UInt64(size = 0)'
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> JoiningTransform: After join block: 'trace_id Nullable(UUID) Nullable(size = 0, UUID(size = 0), UInt8(size = 0)), thecount UInt64 UInt64(size = 0), t.thecount UInt64 UInt64(size = 0)'
[ query-node-1 ] [ 302174 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> Connection (snuba-st-1-2:9000): Sent data for 2 scalars, total 2 rows in 3.9453e-05 sec., 49351 rows/sec., 68.00 B (1.59 MiB/sec.), compressed 0.4594594594594595 times to 148.00 B (3.47 MiB/sec.)
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Debug> executeQuery: (from 10.2.7.138:38110, user: someuser, initial_query_id: 06bba25b-06e9-4014-9207-ddaf79d89cbc) SELECT `trace_id`, count() AS `thecount` FROM `default`.`transactions_local` WHERE (`finish_ts` >= '2024-08-19T10:00:00') AND (`finish_ts` <= '2024-08-19T12:00:00') AND (`project_id` = 2) AND (`trace_id` = 'b18bc43f15e2482e9b7024a5750b502b') GROUP BY `trace_id` (stage: Complete)
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> InterpreterSelectQuery: The min valid primary key position for moving to the tail of PREWHERE is 0
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Debug> InterpreterSelectQuery: MergeTreeWhereOptimizer: condition "(finish_ts >= '2024-08-19T10:00:00') AND (finish_ts <= '2024-08-19T12:00:00') AND (trace_id = 'b18bc43f15e2482e9b7024a5750b502b') AND (project_id = 2)" moved to PREWHERE
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> ContextAccess (someuser): Access granted: SELECT(project_id, trace_id, finish_ts) ON default.transactions_local
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> InterpreterSelectQuery: FetchColumns -> Complete
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Debug> default.transactions_local (c92e3647-dc6d-41ff-b34a-17c10036630b) (SelectExecutor): Key condition: (column 1 in [1724025600, +Inf)), (column 1 in (-Inf, 1724025600]), and, unknown, and, (column 0 in [2, 2]), and
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Debug> default.transactions_local (c92e3647-dc6d-41ff-b34a-17c10036630b) (SelectExecutor): MinMax index condition: (column 0 in [1724061600, +Inf)), (column 0 in (-Inf, 1724068800]), and, unknown, and, unknown, and
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> default.transactions_local (c92e3647-dc6d-41ff-b34a-17c10036630b) (SelectExecutor): Running binary search on index range for part 90-20240819_29039_53905_39 (687 marks)
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> default.transactions_local (c92e3647-dc6d-41ff-b34a-17c10036630b) (SelectExecutor): Found (LEFT) boundary mark: 0
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> default.transactions_local (c92e3647-dc6d-41ff-b34a-17c10036630b) (SelectExecutor): Found (RIGHT) boundary mark: 19
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> default.transactions_local (c92e3647-dc6d-41ff-b34a-17c10036630b) (SelectExecutor): Found continuous range in 18 steps
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Debug> default.transactions_local (c92e3647-dc6d-41ff-b34a-17c10036630b) (SelectExecutor): Index `bf_trace_id` has dropped 18/19 granules.
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Debug> default.transactions_local (c92e3647-dc6d-41ff-b34a-17c10036630b) (SelectExecutor): Selected 1/5 parts by partition key, 1 parts by primary key, 19/68 marks by primary key, 1 marks to read from 1 ranges
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> default.transactions_local (c92e3647-dc6d-41ff-b34a-17c10036630b) (SelectExecutor): Spreading mark ranges among streams (default reading)
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Debug> default.transactions_local (c92e3647-dc6d-41ff-b34a-17c10036630b) (SelectExecutor): Reading approx. 86299 rows with 4 streams
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> MergeTreeBaseSelectProcessor: PREWHERE condition was split into 3 steps: "and(greaterOrEquals(finish_ts, '2024-08-19T10:00:00'), lessOrEquals(finish_ts, '2024-08-19T12:00:00'))", "equals(trace_id, 'b18bc43f15e2482e9b7024a5750b502b')", "equals(project_id, 2)"
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> MergeTreeInOrderSelectProcessor: Reading 1 ranges in order from part 90-20240819_26, approx. 562 rows starting from 0
[ storage-node-2 ] [ 848231 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> AggregatingTransform: Aggregating
[ storage-node-2 ] [ 848231 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> Aggregator: Aggregation method: nullable_keys256
[ storage-node-2 ] [ 848231 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> AggregatingTransform: Aggregated. 1 to 1 rows (from 17.00 B) in 0.024679052 sec. (40.520 rows/sec., 688.84 B/sec.)
[ storage-node-2 ] [ 848231 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> Aggregator: Merging aggregated data
[ storage-node-2 ] [ 848231 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> Aggregator: Statistics updated for key=22023932052865002186: new sum_of_sizes=1, median_size=1
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Debug> executeQuery: Read 562 rows, 73.06 KiB in 0.193038 sec., 18452.325448875352 rows/sec., 378.50 KiB/sec.
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Debug> MemoryTracker: Peak memory usage (for query): 283.90 KiB.
[ storage-node-2 ] [ 1277427 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Debug> TCPHandler: Processed in 0.194109638 sec.
[ query-node-1 ] [ 302174 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> Connection (snuba-st-1-1:9000): Sent data for 2 scalars, total 2 rows in 5.1276e-05 sec., 37869 rows/sec., 68.00 B (1.23 MiB/sec.), compressed 0.4594594594594595 times to 148.00 B (2.66 MiB/sec.)
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Debug> executeQuery: (from 10.2.7.138:38110, user: someuser, initial_query_id: 06bba25b-06e9-4014-9207-ddaf79d89cbc) SELECT `trace_id`, count() AS `thecount` FROM `default`.`errors_local` WHERE (`timestamp` >= '2024-08-19T10:00:00') AND (`timestamp` <= '2024-08-19T12:00:00') AND (`project_id` = 2) AND (`trace_id` = 'b18bc43f15e2482e9b7024a5750b502b') GROUP BY `trace_id` (stage: Complete)
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> InterpreterSelectQuery: The min valid primary key position for moving to the tail of PREWHERE is 0
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Debug> InterpreterSelectQuery: MergeTreeWhereOptimizer: condition "(timestamp >= '2024-08-19T10:00:00') AND (timestamp <= '2024-08-19T12:00:00') AND (trace_id = 'b18bc43f15e2482e9b7024a5750b502b') AND (project_id = 2)" moved to PREWHERE
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> ContextAccess (someuser): Access granted: SELECT(project_id, timestamp, trace_id) ON default.errors_local
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> InterpreterSelectQuery: FetchColumns -> Complete
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Debug> default.errors_local (fb976294-4a9a-43aa-867c-ee1bc9120b57) (SelectExecutor): Key condition: (column 1 in [1724025600, +Inf)), (column 1 in (-Inf, 1724025600]), and, unknown, and, (column 0 in [2, 2]), and
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Debug> default.errors_local (fb976294-4a9a-43aa-867c-ee1bc9120b57) (SelectExecutor): MinMax index condition: (column 0 in [1724061600, +Inf)), (column 0 in (-Inf, 1724068800]), and, unknown, and, unknown, and
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> default.errors_local (fb976294-4a9a-43aa-867c-ee1bc9120b57) (SelectExecutor): Running binary search on index range for part 90-20240819_0_63797_117 (2190 marks)
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> default.errors_local (fb976294-4a9a-43aa-867c-ee1bc9120b57) (SelectExecutor): Found (LEFT) boundary mark: 0
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> default.errors_local (fb976294-4a9a-43aa-867c-ee1bc9120b57) (SelectExecutor): Found (RIGHT) boundary mark: 78
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> default.errors_local (fb976294-4a9a-43aa-867c-ee1bc9120b57) (SelectExecutor): Found continuous range in 22 steps
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Debug> default.errors_local (fb976294-4a9a-43aa-867c-ee1bc9120b57) (SelectExecutor): Selected 1/7 parts by partition key, 1 parts by primary key, 78/89 marks by primary key, 78 marks to read from 1 ranges
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> default.errors_local (fb976294-4a9a-43aa-867c-ee1bc9120b57) (SelectExecutor): Spreading mark ranges among streams (default reading)
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> MergeTreeBaseSelectProcessor: PREWHERE condition was split into 3 steps: "and(greaterOrEquals(timestamp, '2024-08-19T10:00:00'), lessOrEquals(timestamp, '2024-08-19T12:00:00'))", "equals(trace_id, 'b18bc43f15e2482e9b7024a5750b502b')", "equals(project_id, 2)"
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> MergeTreeInOrderSelectProcessor: Reading 1 ranges in order from part 90-20240819_34, approx. 17264 rows starting from 0
[ storage-node-1 ] [ 1287003 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> AggregatingTransform: Aggregated. 0 to 0 rows (from 0.00 B) in 0.03458627 sec. (0.000 rows/sec., 0.00 B/sec.)
[ storage-node-1 ] [ 1287003 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> Aggregator: Merging aggregated data
[ storage-node-1 ] [ 1287003 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Trace> Aggregator: Statistics updated for key=7320201597336447785: new sum_of_sizes=0, median_size=0
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Debug> executeQuery: Read 17264 rows, 41.55 MiB in 0.082856 sec., 27967365.067104373 rows/sec., 501.49 MiB/sec.
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Debug> MemoryTracker: Peak memory usage (for query): 922.92 KiB.
[ storage-node-1 ] [ 1642404 ] {7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec} <Debug> TCPHandler: Processed in 0.097859898 sec.
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> MergingSortedTransform: Merge sorted 2 blocks, 58 rows in 0.464030493 sec., 3142.034892090594 rows/sec., 61.37 KiB/sec
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> executeQuery: Read 20826 rows, 41.62 MiB in 0.300817 sec., 7715075.943181403 rows/sec., 138.36 MiB/sec.
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Trace> HashJoin: (0xbb) Join data is being destroyed, 592 bytes and 1 rows in hash table
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> MemoryTracker: Peak memory usage (for query): 801.15 KiB.
[ query-node-1 ] [ 2303990 ] {06bba25b-06e9-4014-9207-ddaf79d89cbc} <Debug> TCPHandler: Processed in 0.301384176 sec.
    """
