from base import BaseTest

from dataclasses import dataclass
from typing import Any, Mapping, Optional

import datetime

from snuba.datasets.transactions_processor import TransactionsMessageProcessor
from snuba.consumer import KafkaMessageMetadata


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
                'contexts': [
                    {
                        'trace': {
                            'sampled': True,
                            'trace_id': self.trace_id,
                            'op': self.op,
                            'type': 'trace',
                            'span_id': self.span_id,
                        }
                    }
                ],
                'tags': [
                    ['level', 'error'],
                    ['transaction', self.transaction_name],
                    ['sentry:release', self.release],
                    ['sentry:user', self.user_id],
                    ['environment', self.environment],
                    ['trace', self.trace_id],
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

        assert ret[0] == TransactionsMessageProcessor.INSERT
        assert ret[1] == {
            'deleted': 0,
            'event_id': 'e5e062bf-2e1d-4afd-96fd-2f90b6770431',
            'trace_id': '7400045b-25c4-43b8-8591-4600aa83ad04',
            'span_id': 9818240959241804171,
            'transaction_name': '/organizations/:orgId/issues/',
            'transaciton_op': 'navigation',
            'start_ts': datetime.fromtimestamp(1565303393),
            'start_ms': 917,
            'finish_ts': datetime.fromtimestamp(1565303392),
            'finish_ms': 918,
            'platform': 'python',
            'environment': 'prod',
            'release': '34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a',
            'dist': None,
            'ip_address_v4': '127.0.0.1',
            'ip_address_v6': None,
            'user': None,
            'user_id': 'myself',
            'user_name': 'me',
            'user_email': 'me@myself.com',
            'tags': [
                ['level', 'error'],
                ['transaction', '/organizations/:orgId/issues/'],
                ['sentry:release', '34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a'],
                ['sentry:user', 'myself'],
            ],
            'contexts': [
                {
                    'trace': {
                        'sampled': True,
                        'start_timestamp': 1565303393.917,
                        'transaction': '/organizations/:orgId/issues/',
                        'timestamp': 1565303392.918,
                        'trace_id': '7400045b25c443b885914600aa83ad04',
                        'op': 'navigation',
                        'span_id': '8841662216cc598b',
                    }
                }
            ],
            'offset': 1,
            'partition': 2,
            'retention_days': 1
        }
