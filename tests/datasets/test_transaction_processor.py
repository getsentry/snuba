import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence, Tuple
from unittest.mock import ANY

import pytest

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.transactions_processor import (
    TransactionsMessageProcessor,
)
from snuba.processor import InsertBatch
from snuba.state import set_config


@dataclass
class TransactionEvent:
    event_id: str
    trace_id: str
    span_id: str
    group_ids: Sequence[int]
    transaction_name: str
    op: str
    start_timestamp: float
    timestamp: float
    platform: str
    dist: Optional[str]
    user_name: Optional[str]
    user_id: Optional[str]
    user_email: Optional[str]
    ipv6: Optional[str]
    ipv4: Optional[str]
    environment: Optional[str]
    release: str
    sdk_name: Optional[str]
    sdk_version: Optional[str]
    http_method: Optional[str]
    http_referer: Optional[str]
    geo: Mapping[str, str]
    status: str
    transaction_source: Optional[str]
    app_start_type: str = "warm"
    has_app_ctx: bool = True
    profile_id: Optional[str] = None
    replay_id: Optional[str] = None
    received: Optional[float] = None

    def get_app_context(self) -> Optional[Mapping[str, str]]:
        if self.has_app_ctx:
            return {"start_type": self.app_start_type}
        else:
            return None

    def get_profile_context(self) -> Optional[Mapping[str, str]]:
        if self.profile_id is None:
            return None
        return {"profile_id": self.profile_id}

    def get_replay_context(self) -> Optional[Mapping[str, str]]:
        if self.replay_id is None:
            return None
        return {"replay_id": self.replay_id}

    def serialize(self) -> Tuple[int, str, Mapping[str, Any]]:
        return (
            2,
            "insert",
            {
                "datetime": "2019-08-08T22:29:53.917000Z",
                "organization_id": 1,
                "platform": self.platform,
                "project_id": 1,
                "event_id": self.event_id,
                "message": "/organizations/:orgId/issues/",
                "group_id": None,
                "group_ids": self.group_ids,
                "retention_days": 23,
                "data": {
                    "event_id": self.event_id,
                    "environment": self.environment,
                    "project_id": 1,
                    "release": self.release,
                    "dist": self.dist,
                    "transaction_info": {"source": "url"},
                    "grouping_config": {
                        "enhancements": "eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC",
                        "id": "legacy:2019-03-12",
                    },
                    "sdk": {
                        "version": self.sdk_version,
                        "name": self.sdk_name,
                        "packages": [{"version": "0.9.0", "name": "pypi:sentry-sdk"}],
                    },
                    "breadcrumbs": {
                        "values": [
                            {
                                "category": "query",
                                "timestamp": 1565308204.544,
                                "message": "[Filtered]",
                                "type": "default",
                                "level": "info",
                            },
                        ],
                    },
                    "spans": [
                        {
                            "sampled": True,
                            "start_timestamp": self.start_timestamp,
                            "same_process_as_parent": None,
                            "description": "GET /api/0/organizations/sentry/tags/?project=1",
                            "tags": None,
                            "timestamp": 1565303389.366,
                            "parent_span_id": self.span_id,
                            "trace_id": self.trace_id,
                            "span_id": "b70840cd33074881",
                            "data": {},
                            "op": "http",
                            "hash": "b" * 16,
                            "exclusive_time": 0.1234,
                        }
                    ],
                    "platform": self.platform,
                    "version": "7",
                    "location": "/organizations/:orgId/issues/",
                    "logger": "",
                    "type": "transaction",
                    "metadata": {
                        "location": "/organizations/:orgId/issues/",
                        "title": "/organizations/:orgId/issues/",
                    },
                    "primary_hash": "d41d8cd98f00b204e9800998ecf8427e",
                    "datetime": "2019-08-08T22:29:53.917000Z",
                    "timestamp": self.timestamp,
                    "start_timestamp": self.start_timestamp,
                    "received": self.received,
                    "measurements": {
                        "lcp": {"value": 32.129},
                        "lcp.elementSize": {"value": 4242},
                        "fid": {"value": None},
                        "invalid": None,
                        "invalid2": {},
                    },
                    "breakdowns": {
                        "span_ops": {
                            "ops.db": {"value": 62.512},
                            "ops.http": {"value": 109.774},
                            "total.time": {"value": 172.286},
                        }
                    },
                    "contexts": {
                        "trace": {
                            "sampled": True,
                            "trace_id": self.trace_id,
                            "op": self.op,
                            "type": "trace",
                            "span_id": self.span_id,
                            "status": self.status,
                            "hash": "a" * 16,
                            "exclusive_time": 1.2345,
                        },
                        "app": self.get_app_context(),
                        "experiments": {"test1": 1, "test2": 2},
                        "profile": self.get_profile_context(),
                        "replay": self.get_replay_context(),
                    },
                    "tags": [
                        ["sentry:release", self.release],
                        ["sentry:user", self.user_id],
                        ["environment", self.environment],
                        ["we|r=d", "tag"],
                    ],
                    "user": {
                        "username": self.user_name,
                        "ip_address": self.ipv4 or self.ipv6,
                        "id": self.user_id,
                        "email": self.user_email,
                        "geo": self.geo,
                    },
                    "request": {
                        "url": "http://127.0.0.1:/query",
                        "headers": [
                            ["Accept-Encoding", "identity"],
                            ["Content-Length", "398"],
                            ["Host", "127.0.0.1:"],
                            ["Referer", self.http_referer],
                            ["Trace", "8fa73032d-1"],
                        ],
                        "data": "",
                        "method": self.http_method,
                        "env": {"SERVER_PORT": "1010", "SERVER_NAME": "snuba"},
                    },
                    "transaction": self.transaction_name,
                },
            },
        )

    def build_result(self, meta: KafkaMessageMetadata) -> Mapping[str, Any]:
        start_timestamp = datetime.utcfromtimestamp(self.start_timestamp)
        finish_timestamp = datetime.utcfromtimestamp(self.timestamp)

        spans = sorted(
            [(self.op, int("a" * 16, 16), 1.2345), ("http", int("b" * 16, 16), 0.1234)]
        )

        ret = {
            "deleted": 0,
            "project_id": 1,
            "event_id": str(uuid.UUID(self.event_id)),
            "trace_id": str(uuid.UUID(self.trace_id)),
            "span_id": int(self.span_id, 16),
            "group_ids": self.group_ids,
            "transaction_name": self.transaction_name,
            "transaction_op": self.op,
            "transaction_status": 1 if self.status == "cancelled" else 2,
            "transaction_source": "url",
            "start_ts": start_timestamp,
            "start_ms": int(start_timestamp.microsecond / 1000),
            "finish_ts": finish_timestamp,
            "finish_ms": int(finish_timestamp.microsecond / 1000),
            "duration": int(
                (finish_timestamp - start_timestamp).total_seconds() * 1000
            ),
            "platform": self.platform,
            "environment": self.environment,
            "release": self.release,
            "dist": self.dist,
            "user": self.user_id,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "user_email": self.user_email,
            "tags.key": ["environment", "sentry:release", "sentry:user", "we|r=d"],
            "tags.value": [self.environment, self.release, self.user_id, "tag"],
            # Notice that we do not store trace.trace_id or trace.span_id in contexts
            # this is because it is redundant (as it is also stored as a promoted column)
            "contexts.key": [
                "trace.sampled",
                "trace.op",
                "trace.status",
                "geo.country_code",
                "geo.region",
                "geo.city",
                "geo.subdivision",
            ],
            "contexts.value": [
                "True",
                self.op,
                self.status,
                self.geo["country_code"],
                self.geo["region"],
                self.geo["city"],
                self.geo["subdivision"],
            ],
            "sdk_name": "sentry.python",
            "sdk_version": "0.9.0",
            "http_method": self.http_method,
            "http_referer": self.http_referer,
            "offset": meta.offset,
            "partition": meta.partition,
            "retention_days": 30,
            "measurements.key": ["lcp", "lcp.elementSize"],
            "measurements.value": [32.129, 4242.0],
            "span_op_breakdowns.key": ["ops.db", "ops.http", "total.time"],
            "span_op_breakdowns.value": [62.512, 109.774, 172.286],
            "spans.op": [span[0] for span in spans],
            "spans.group": [span[1] for span in spans],
            "spans.exclusive_time": [0 for span in spans],
            "spans.exclusive_time_32": [span[2] for span in spans],
        }

        if self.has_app_ctx:
            ret["app_start_type"] = self.app_start_type

        if self.ipv4:
            ret["ip_address_v4"] = self.ipv4
        else:
            ret["ip_address_v6"] = self.ipv6

        if self.profile_id is not None:
            ret["profile_id"] = str(uuid.UUID(self.profile_id))
        if self.replay_id is not None:
            ret["replay_id"] = str(uuid.UUID(self.replay_id))
            ret["tags.key"].append("replayId")
            ret["tags.value"].append(self.replay_id)

        return ret


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTransactionsProcessor:
    def __get_timestamps(self) -> Tuple[float, float]:
        timestamp = datetime.now(tz=timezone.utc) - timedelta(seconds=5)
        start_timestamp = timestamp - timedelta(seconds=5)
        return (start_timestamp.timestamp(), timestamp.timestamp())

    def __get_transaction_event(self) -> TransactionEvent:
        start, finish = self.__get_timestamps()
        return TransactionEvent(
            event_id="e5e062bf2e1d4afd96fd2f90b6770431",
            trace_id="7400045b25c443b885914600aa83ad04",
            span_id="8841662216cc598b",
            group_ids=[100, 200],
            transaction_name="/organizations/:orgId/issues/",
            status="cancelled",
            op="navigation",
            timestamp=finish,
            start_timestamp=start,
            platform="python",
            dist="",
            user_name="me",
            user_id="myself",
            user_email="me@myself.com",
            ipv4="127.0.0.1",
            ipv6=None,
            environment="prod",
            release="34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a",
            sdk_name="sentry.python",
            sdk_version="0.9.0",
            http_method="POST",
            http_referer="tagstore.something",
            geo={
                "country_code": "XY",
                "region": "fake_region",
                "city": "fake_city",
                "subdivision": "fake_subdivision",
            },
            transaction_source="url",
            profile_id="046852d24483455c8c44f0c8fbf496f9",
            replay_id="d2731f8ed8934c6fa5253e450915aa12",
        )

    def test_skip_non_transactions(self) -> None:
        message = self.__get_transaction_event()
        payload = message.serialize()
        # Force an invalid event
        payload[2]["data"]["type"] = "error"

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        processor = TransactionsMessageProcessor()
        assert processor.process_message(payload, meta) is None

    def test_missing_trace_context(self) -> None:
        message = self.__get_transaction_event()
        payload = message.serialize()
        # Force an invalid event
        del payload[2]["data"]["contexts"]

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        processor = TransactionsMessageProcessor()
        assert processor.process_message(payload, meta) is None

    def test_base_process(self) -> None:
        old_skip_context = settings.TRANSACT_SKIP_CONTEXT_STORE
        settings.TRANSACT_SKIP_CONTEXT_STORE = {1: {"experiments"}}

        message = self.__get_transaction_event()

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        assert TransactionsMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch([message.build_result(meta)], ANY)
        settings.TRANSACT_SKIP_CONTEXT_STORE = old_skip_context

    def test_too_many_spans(self) -> None:
        old_skip_context = settings.TRANSACT_SKIP_CONTEXT_STORE
        settings.TRANSACT_SKIP_CONTEXT_STORE = {1: {"experiments"}}
        set_config("max_spans_per_transaction", 1)

        message = self.__get_transaction_event()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )

        payload = message.serialize()

        # there are 2 spans in the transaction but only 1
        # will be inserted because of the limit set above
        result = message.build_result(meta)
        result["spans.op"] = ["navigation"]
        result["spans.group"] = [int("a" * 16, 16)]
        result["spans.exclusive_time"] = [0]
        result["spans.exclusive_time_32"] = [1.2345]

        assert TransactionsMessageProcessor().process_message(
            payload, meta
        ) == InsertBatch([result], ANY)
        settings.TRANSACT_SKIP_CONTEXT_STORE = old_skip_context

    def test_missing_transaction_source(self) -> None:
        message = self.__get_transaction_event()
        message.transaction_source = ""

        payload_base = message.serialize()
        payload_wo_transaction_info = deepcopy(payload_base)
        payload_wo_source = deepcopy(payload_base)
        # Remove transaction_info
        del payload_wo_transaction_info[2]["data"]["transaction_info"]

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        actual_message = TransactionsMessageProcessor().process_message(
            payload_wo_transaction_info, meta
        )
        assert actual_message.rows[0]["transaction_source"] == ""

        # Remove transaction_info.source
        del payload_wo_source[2]["data"]["transaction_info"]["source"]

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        actual_message = TransactionsMessageProcessor().process_message(
            payload_wo_source, meta
        )
        assert actual_message.rows[0]["transaction_source"] == ""

    def test_app_ctx_none(self) -> None:
        old_skip_context = settings.TRANSACT_SKIP_CONTEXT_STORE
        settings.TRANSACT_SKIP_CONTEXT_STORE = {1: {"experiments"}}

        message = self.__get_transaction_event()
        message.has_app_ctx = False

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        assert TransactionsMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch([message.build_result(meta)], ANY)
        settings.TRANSACT_SKIP_CONTEXT_STORE = old_skip_context

    def test_replay_id_as_tag(self) -> None:
        # we should write both the tag and the top level field
        # if replay_id is sent as a tag
        settings.TRANSACT_SKIP_CONTEXT_STORE = {1: {"experiments"}}

        message = self.__get_transaction_event()
        payload = message.serialize()

        payload[2]["data"]["tags"].append(
            ["replayId", "d2731f8ed8934c6fa5253e450915aa12"]
        )
        del payload[2]["data"]["contexts"]["replay"]

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        result = message.build_result(meta)

        # when the replay_id is sent as a tag instead of a context,
        # the order in which it's placed in the tags list is different
        result["tags.key"].remove("replayId")
        result["tags.value"].remove("d2731f8ed8934c6fa5253e450915aa12")
        result["tags.key"].insert(1, "replayId")
        result["tags.value"].insert(1, "d2731f8ed8934c6fa5253e450915aa12")

        assert TransactionsMessageProcessor().process_message(
            payload, meta
        ) == InsertBatch([result], ANY)

    def test_replay_id_as_tag_and_context(self) -> None:
        """
        replays shoud only ever have the context set, or the tag set, not both
        but just in case, ensure that we don't write two replay_id tags
        """
        settings.TRANSACT_SKIP_CONTEXT_STORE = {1: {"experiments"}}

        message = self.__get_transaction_event()
        payload = message.serialize()

        payload[2]["data"]["tags"].append(
            ["replayId", "d2731f8ed8934c6fa5253e450915aa12"]
        )

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        result = message.build_result(meta)

        # when the replay_id is sent as a tag instead of a context,
        # the order in which it's placed in the tags list is different
        result["tags.key"].remove("replayId")
        result["tags.value"].remove("d2731f8ed8934c6fa5253e450915aa12")
        result["tags.key"].insert(1, "replayId")
        result["tags.value"].insert(1, "d2731f8ed8934c6fa5253e450915aa12")

        assert TransactionsMessageProcessor().process_message(
            payload, meta
        ) == InsertBatch([result], ANY)

    def test_replay_id_as_invalid_tag(self) -> None:
        """
        if a replayId is sent as an invalid uuid, don't try to set it on the context,
        and keep it as a tag
        """
        settings.TRANSACT_SKIP_CONTEXT_STORE = {1: {"experiments"}}

        message = self.__get_transaction_event()
        payload = message.serialize()
        del payload[2]["data"]["contexts"]["replay"]
        payload[2]["data"]["tags"].append(["replayId", "I_AM_NOT_A_UUID"])

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        result = message.build_result(meta)

        del result["replay_id"]
        result["tags.key"].remove("replayId")
        result["tags.value"].remove("d2731f8ed8934c6fa5253e450915aa12")
        result["tags.key"].insert(1, "replayId")
        result["tags.value"].insert(1, "I_AM_NOT_A_UUID")

        assert TransactionsMessageProcessor().process_message(
            payload, meta
        ) == InsertBatch([result], ANY)
