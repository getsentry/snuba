from __future__ import annotations

# import uuid
# from dataclasses import asdict, dataclass
from datetime import datetime

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.replays_processor import ReplaysProcessor
from snuba.processor import InsertBatch

# from typing import Any, Mapping, Optional


# @dataclass
# class ReplayEvent:
#     project_id: int
#     replay_id: str
#     sequence_id: int
#     trace_ids: list[str]
#     timestamp: datetime
#     platform: str
#     environment: str
#     release: str
#     dist: str
#     ip_address_v4: str
#     ip_address_v6: str | None
#     user: str
#     sdk_name: str
#     sdk_version: str
#     tags: list[Mapping[str, Any]]
#     retention_days: int
#     title: str
#     offset: int
#     partition: int

#     def serialize(self) -> Mapping[str, Any]:
#         return asdict(self)

#     def build_result(self, meta: KafkaMessageMetadata) -> Mapping[str, Any]:
#         result = asdict(self)
#         result["offset"] = meta.offset
#         result["partition"] = meta.partition
#         return result


class TestReplaysProcessor:
    def test_process_message(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )
        message = {
            "datetime": 1649965212.374942,
            "platform": "javascript",
            "project_id": 7,
            "event_id": "163c16e78a5049a5b64829cc1e263d9b",
            "data": {
                "event_id": "163c16e78a5049a5b64829cc1e263d9b",
                "level": "error",
                "version": "7",
                "type": "replay_event",
                "transaction": "sentry-replay",
                "logger": "",
                "platform": "javascript",
                "timestamp": 1649965212.374942,
                "start_timestamp": 1649965212.374942,
                "received": 1649965212.376442,
                "environment": "production",
                "user": {"ip_address": "127.0.0.1"},
                "request": {
                    "url": "https://75030aaee25f.ngrok.io/",
                    "headers": [
                        [
                            "User-Agent",
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36",
                        ]
                    ],
                },
                "contexts": {
                    "browser": {
                        "name": "Chrome",
                        "version": "100.0.4896",
                        "type": "browser",
                    },
                    "device": {
                        "family": "Mac",
                        "model": "Mac",
                        "brand": "Apple",
                        "type": "device",
                    },
                    "os": {"name": "Mac OS X", "version": "10.15.7", "type": "os"},
                    "trace": {
                        "trace_id": "ffb5344a41dd4b219288187a2cd1ad6d",
                        "span_id": "ba1586800b803058",
                        "status": "unknown",
                        "tags": {"isReplayRoot": "yes"},
                        "type": "trace",
                    },
                },
                "breadcrumbs": {
                    "values": [
                        {
                            "timestamp": 1649965212.328442,
                            "type": "default",
                            "category": "console",
                            "level": "info",
                            "message": "%c prev state color: #9E9E9E; font-weight: bold [object Object]",
                            "data": {
                                "arguments": [
                                    "%c prev state",
                                    "color: #9E9E9E; font-weight: bold",
                                    {
                                        "article": "[Object]",
                                        "articleList": "[Object]",
                                        "auth": "[Filtered]",
                                        "common": "[Object]",
                                        "editor": "[Object]",
                                        "home": "[Object]",
                                        "profile": "[Object]",
                                        "router": "[Object]",
                                        "settings": "[Object]",
                                    },
                                ],
                                "logger": "console",
                            },
                        }
                    ]
                },
                "tags": [
                    ["isReplayRoot", "yes"],
                    ["skippedNormalization", "True"],
                    ["transaction", "/"],
                ],
                "extra": {"normalizeDepth": 3},
                "sdk": {
                    "name": "sentry.javascript.react",
                    "version": "6.18.1",
                    "integrations": [
                        "InboundFilters",
                        "FunctionToString",
                        "TryCatch",
                        "Breadcrumbs",
                        "GlobalHandlers",
                        "LinkedErrors",
                        "Dedupe",
                        "UserAgent",
                        "Replay",
                        "BrowserTracing",
                    ],
                    "packages": [{"name": "npm:@sentry/react", "version": "6.18.1"}],
                },
                "errors": [
                    {
                        "type": "clock_drift",
                        "name": "timestamp",
                        "sdk_time": "2022-04-07T20:14:38.483+00:00",
                        "server_time": "2022-04-14T19:40:12.376441670+00:00",
                    }
                ],
                "key_id": "3",
                "project": 7,
                "grouping_config": {
                    "enhancements": "eJybzDRxY3J-bm5-npWRgaGlroGxrpHxBABcYgcZ",
                    "id": "newstyle:2019-10-29",
                },
                "spans": [],
                "_metrics": {"bytes.ingested.event": 8793, "sample_rates": [{}]},
                "_meta": {
                    "breadcrumbs": {
                        "values": {
                            "0": {
                                "data": {
                                    "arguments": {
                                        "2": {
                                            "auth": {
                                                "": {
                                                    "rem": [
                                                        ["@password:filter", "s", 0, 10]
                                                    ],
                                                    "len": 8,
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "timestamp": {
                        "": {
                            "err": [
                                [
                                    "clock_drift",
                                    {
                                        "sdk_time": "2022-04-07T20:14:38.483+00:00",
                                        "server_time": "2022-04-14T19:40:12.376441670+00:00",
                                    },
                                ]
                            ]
                        }
                    },
                },
            },
            "retention_days": 30,
        }
        assert ReplaysProcessor().process_message(message, meta) == InsertBatch(
            [
                {
                    "deleted": 0,
                    "retention_days": 30,
                    "event_id": "163c16e7-8a50-49a5-b648-29cc1e263d9b",
                    "project_id": 7,
                    "start_ts": datetime(2022, 4, 14, 19, 40, 12, 374942),
                    "start_ms": 374,
                    "finish_ts": datetime(2022, 4, 14, 19, 40, 12, 374942),
                    "finish_ms": 374,
                    "duration": 0,
                    "platform": "javascript",
                    "tags.key": ["isReplayRoot", "skippedNormalization", "transaction"],
                    "tags.value": ["yes", "True", "/"],
                    "release": None,
                    "environment": None,
                    "user": "",
                    "dist": None,
                    "sdk_name": "sentry.javascript.react",
                    "sdk_version": "6.18.1",
                    "user_name": None,
                    "user_id": None,
                    "user_email": None,
                    "ip_address_v4": "127.0.0.1",
                }
            ],
            None,
        )
