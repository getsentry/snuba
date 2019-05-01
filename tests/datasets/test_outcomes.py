from functools import partial
import json

from snuba import settings, state
from snuba.consumer import ConsumerWorker

from tests.base import BaseTest, FakeBatchingKafkaConsumer
from snuba.perf import FakeKafkaMessage

class TestOutcomes(BaseTest):
    @classmethod
    def setup_class(cls):
        cls.dataset = settings.get_dataset('outcomes')

    def setup_method(self, test_method):
        super(TestOutcomes, self).setup_method(test_method)
        from snuba.api import application
        assert application.testing is True
        application.config['PROPAGATE_EXCEPTIONS'] = False

        self.app = application.test_client()
        self.app.post = partial(self.app.post, headers={'referer': 'test'})

    def _wrap(self, msg, offset):
        return FakeKafkaMessage(
            self.dataset.PROCESSOR.MESSAGE_TOPIC, 0,
            offset, json.dumps(msg).encode('utf-8')
        )

    def test_table(self):
        res = self.clickhouse.execute("SELECT count() FROM %s" % self.dataset.SCHEMA.QUERY_TABLE)
        assert res[0][0] == 0

    def test_simple_e2e(self):
        """
        Test that messages from kafka can be consumed and written
        to clickhouse, and the resulting rows can be queried by the
        API.
        """
        topic = self.dataset.PROCESSOR.MESSAGE_TOPIC
        worker = ConsumerWorker(self.clickhouse, self.dataset, None, topic)
        consumer = FakeBatchingKafkaConsumer(
            topic,
            worker=worker,
            max_batch_size=2,
            max_batch_time=100,
            bootstrap_servers=None,
            group_id=self.dataset.PROCESSOR.MESSAGE_CONSUMER_GROUP,
        )

        consumer.consumer.items = [self._wrap(msg, i) for i, msg in enumerate([
            {"timestamp": "2019-04-29T22:46:21.463938Z", "org_id": 1, "project_id": 2, "key_id": 3, "outcome": 'accepted', "reason": None},
            {"timestamp": "2019-04-29T22:46:21.500000Z", "org_id": 1, "project_id": 2, "key_id": 3, "outcome": 3, "reason": 'too_large'},
        ])]

        #run_once because run() won't return
        for _ in range(len(consumer.consumer.items)):
            consumer._run_once()
        consumer._flush()
        consumer._shutdown()

        result = json.loads(self.app.post('/query', data=json.dumps({
            'from_date': "2019-04-29T00:00:00Z",
            'to_date': "2019-04-30T00:00:00Z",
            'project': 2,
            'selected_columns': ['outcome', 'reason'],
            'dataset': 'outcomes'
        })).data)
        assert 'error' not in result
        assert len(result['data']) == 2
        assert set([d['outcome'] for d in result['data']]) == {0, 3}
        assert set([d['reason'] for d in result['data']]) == {None, 'too_large'}
