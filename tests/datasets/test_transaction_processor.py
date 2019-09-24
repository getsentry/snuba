from base import BaseTest

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from datetime import datetime

import uuid

from snuba.datasets.transactions_processor import TransactionsMessageProcessor
from snuba.consumer import KafkaMessageMetadata
from snuba.processor import ProcessorAction


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

    def serialize(self) -> Mapping[str, Any]:
        return (2, 'insert', {
            'datetime': '2019-08-08T22:29:53.917000Z',
            'organization_id': 1,
            'platform': self.platform,
            'project_id': 1,
            'event_id': self.event_id,
            'message': '/organizations/:orgId/issues/',
            'group_id': None,
            'data': {
                'event_id': self.event_id,
                'environment': self.environment,
                'project_id': 1,
                'release': self.release,
                'dist': self.dist,
                'grouping_config': {
                    'enhancements': 'eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC',
                    'id': 'legacy:2019-03-12',
                },
                'breadcrumbs': {
                    'values': [
                        {
                            'category': 'query',
                            'timestamp': 1565308204.544,
                            'message': '[Filtered]',
                            'type': 'default',
                            'level': 'info'
                        },
                    ],
                },
                'spans': [
                    {
                        'sampled': True,
                        'start_timestamp': self.start_timestamp,
                        'same_process_as_parent': None,
                        'description': 'GET /api/0/organizations/sentry/tags/?project=1',
                        'tags': None,
                        'timestamp': 1565303389.366,
                        'parent_span_id': self.span_id,
                        'trace_id': self.trace_id,
                        'span_id': 'b70840cd33074881',
                        'data': {},
                        'op': 'http'
                    }
                ],
                'platform': self.platform,
                'version': '7',
                'location': '/organizations/:orgId/issues/',
                'logger': '',
                'type': 'transaction',
                'metadata': {
                    'location': '/organizations/:orgId/issues/',
                    'title': '/organizations/:orgId/issues/',
                },
                'primary_hash': 'd41d8cd98f00b204e9800998ecf8427e',
                'retention_days': None,
                'datetime': '2019-08-08T22:29:53.917000Z',
                'timestamp': self.timestamp,
                'start_timestamp': self.start_timestamp,
                'contexts': {
                    'trace': {
                        'sampled': True,
                        'trace_id': self.trace_id,
                        'op': self.op,
                        'type': 'trace',
                        'span_id': self.span_id,
                    }
                },
                'tags': [
                    ['sentry:release', self.release],
                    ['sentry:user', self.user_id],
                    ['environment', self.environment],
                ],
                'user': {
                    'username': self.user_name,
                    'ip_address': self.ipv4 or self.ipv6,
                    'id': self.user_id,
                    'email': self.user_email,
                },
                'transaction': self.transaction_name,
            }
        })

    def build_result(self, meta: KafkaMessageMetadata) -> Mapping[str, Any]:
        start_timestamp = datetime.fromtimestamp(self.start_timestamp)
        finish_timestamp = datetime.fromtimestamp(self.timestamp)

        ret = {
            'deleted': 0,
            'project_id': 1,
            'event_id': str(uuid.UUID(self.event_id)),
            'trace_id': str(uuid.UUID(self.trace_id)),
            'span_id': int(self.span_id, 16),
            'transaction_name': self.transaction_name,
            'transaction_op': self.op,
            'start_ts': start_timestamp,
            'start_ms': int(start_timestamp.microsecond / 1000),
            'finish_ts': finish_timestamp,
            'finish_ms': int(finish_timestamp.microsecond / 1000),
            'platform': self.platform,
            'environment': self.environment,
            'release': self.release,
            'dist': self.dist,
            'user': self.user_id,
            'user_id': self.user_id,
            'user_name': self.user_name,
            'user_email': self.user_email,
            'tags.key': ['environment', 'sentry:release', 'sentry:user'],
            'tags.value': [
                self.environment,
                self.release,
                self.user_id,
            ],
            'contexts.key': [
                'trace.sampled', 'trace.trace_id',
                'trace.op', 'trace.span_id',
            ],
            'contexts.value': [
                'True', self.trace_id, self.op, self.span_id
            ],
            'offset': meta.offset,
            'partition': meta.partition,
            'retention_days': 90,
        }

        if self.ipv4:
            ret['ip_address_v4'] = self.ipv4
        else:
            ret['ip_address_v6'] = self.ipv6
        return ret


class TestTransactionsProcessor(BaseTest):

    def test_base_process(self):
        message = TransactionEvent(
            event_id='e5e062bf2e1d4afd96fd2f90b6770431',
            trace_id='7400045b25c443b885914600aa83ad04',
            span_id='8841662216cc598b',
            transaction_name='/organizations/:orgId/issues/',
            op='navigation',
            start_timestamp=1565303393.917,
            timestamp=1565303392.918,
            platform='python',
            dist='',
            user_name='me',
            user_id='myself',
            user_email='me@myself.com',
            ipv4='127.0.0.1',
            ipv6=None,
            environment='prod',
            release='34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a',
        )
        meta = KafkaMessageMetadata(
            offset=1,
            partition=2,
        )

        processor = TransactionsMessageProcessor()
        ret = processor.process_message(message.serialize(), meta)

        assert ret[0] == ProcessorAction.INSERT
        assert ret[1] == [message.build_result(meta)]
