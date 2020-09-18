import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Tuple

from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.transactions_processor import TransactionsMessageProcessor
from snuba.processor import InsertBatch
from tests.base import BaseTest


@dataclass
class TransactionEvent:
    event_id: str
    trace_id: str
    span_id: str
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

    def serialize(self) -> Mapping[str, Any]:
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
                "data": {
                    "event_id": self.event_id,
                    "environment": self.environment,
                    "project_id": 1,
                    "release": self.release,
                    "dist": self.dist,
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
                    "retention_days": None,
                    "datetime": "2019-08-08T22:29:53.917000Z",
                    "timestamp": self.timestamp,
                    "start_timestamp": self.start_timestamp,
                    "measurements": {
                        "lcp": {"value": 32.129},
                        "lcp.elementSize": {"value": 4242},
                    },
                    "contexts": {
                        "trace": {
                            "sampled": True,
                            "trace_id": self.trace_id,
                            "op": self.op,
                            "type": "trace",
                            "span_id": self.span_id,
                            "status": self.status,
                        },
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

        ret = {
            "deleted": 0,
            "project_id": 1,
            "event_id": str(uuid.UUID(self.event_id)),
            "trace_id": str(uuid.UUID(self.trace_id)),
            "span_id": int(self.span_id, 16),
            "transaction_name": self.transaction_name,
            "transaction_op": self.op,
            "transaction_status": 1 if self.status == "cancelled" else 2,
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
            "contexts.key": [
                "trace.sampled",
                "trace.trace_id",
                "trace.op",
                "trace.span_id",
                "trace.status",
                "geo.country_code",
                "geo.region",
                "geo.city",
            ],
            "contexts.value": [
                "True",
                self.trace_id,
                self.op,
                self.span_id,
                self.status,
                self.geo["country_code"],
                self.geo["region"],
                self.geo["city"],
            ],
            "sdk_name": "sentry.python",
            "sdk_version": "0.9.0",
            "http_method": self.http_method,
            "http_referer": self.http_referer,
            "offset": meta.offset,
            "partition": meta.partition,
            "retention_days": 90,
            "_tags_flattened": f"|environment={self.environment}||sentry:release={self.release}||sentry:user={self.user_id}||we\\|r\\=d=tag|",
            "_contexts_flattened": (
                f"|geo.city={self.geo['city']}||geo.country_code={self.geo['country_code']}||geo.region={self.geo['region']}|"
                f"|trace.op={self.op}||trace.sampled=True||trace.span_id={self.span_id}||trace.status={str(self.status)}|"
                f"|trace.trace_id={self.trace_id}|"
            ),
            "measurements.key": ["lcp", "lcp.elementSize"],
            "measurements.value": [32.129, 4242.0],
        }

        if self.ipv4:
            ret["ip_address_v4"] = self.ipv4
        else:
            ret["ip_address_v6"] = self.ipv6
        return ret


class TestTransactionsProcessor(BaseTest):
    def __get_timestamps(slef) -> Tuple[float, float]:
        timestamp = datetime.now(tz=timezone.utc) - timedelta(seconds=5)
        start_timestamp = timestamp - timedelta(seconds=5)
        return (start_timestamp.timestamp(), timestamp.timestamp())

    def test_skip_non_transactions(self):
        start, finish = self.__get_timestamps()
        message = TransactionEvent(
            event_id="e5e062bf2e1d4afd96fd2f90b6770431",
            trace_id="7400045b25c443b885914600aa83ad04",
            span_id="8841662216cc598b",
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
            geo={"country_code": "XY", "region": "fake_region", "city": "fake_city"},
        )
        payload = message.serialize()
        # Force an invalid event
        payload[2]["data"]["type"] = "error"

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        processor = TransactionsMessageProcessor()
        assert processor.process_message(payload, meta) is None

    def test_missing_trace_context(self):
        start, finish = self.__get_timestamps()
        message = TransactionEvent(
            event_id="e5e062bf2e1d4afd96fd2f90b6770431",
            trace_id="7400045b25c443b885914600aa83ad04",
            span_id="8841662216cc598b",
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
            geo={"country_code": "XY", "region": "fake_region", "city": "fake_city"},
        )
        payload = message.serialize()
        # Force an invalid event
        del payload[2]["data"]["contexts"]

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        processor = TransactionsMessageProcessor()
        assert processor.process_message(payload, meta) is None

    def test_base_process(self):
        start, finish = self.__get_timestamps()
        message = TransactionEvent(
            event_id="e5e062bf2e1d4afd96fd2f90b6770431",
            trace_id="7400045b25c443b885914600aa83ad04",
            span_id="8841662216cc598b",
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
            geo={"country_code": "XY", "region": "fake_region", "city": "fake_city"},
        )
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        assert TransactionsMessageProcessor().process_message(
            message.serialize(), meta
        ) == InsertBatch([message.build_result(meta)])
