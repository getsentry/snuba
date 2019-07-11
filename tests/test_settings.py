from base import BaseTest
from snuba.settings import DATASETS, load_dataset_settings
from snuba.settings_types import (
    DatasetSettings,
    DatasetConsumerSettings,
    KafkaTopicSettings,
    KafkaClusterSettings,
    deep_copy_and_merge,
)


class TestSettings(BaseTest):

    def test_base_load(self):
        events_config = load_dataset_settings('events')
        expected = DatasetSettings(
            consumer=DatasetConsumerSettings(
                kafka_cluster=KafkaClusterSettings(
                    brokers=['localhost:9093'],
                    max_batch_size=50000,
                    max_batch_time_ms=2000,
                    queued_max_message_kbytes=50000,
                    queued_min_messages=20000,
                ),
                message_topic=KafkaTopicSettings(
                    name='events',
                    consumer_group='snuba_consumers',
                ),
                commit_log_topic='snuba_commit_log',
                replacement_topic='event-replacements',
            )
        )

        assert expected == events_config

    def test_load_override_setting(self):
        DATASETS['events']['consumer']['kafka_cluster_override'] = {
            "queued_max_message_kbytes": 0,  # ensure 0 works
            "queued_min_messages": 1,
        }
        DATASETS['events']['consumer']['message_topic'] = {
            'name': 'events2',
            'consumer_group': 'snuba_consumers2',
        }

        events_config = load_dataset_settings('events')
        expected = DatasetSettings(
            consumer=DatasetConsumerSettings(
                kafka_cluster=KafkaClusterSettings(
                    brokers=['localhost:9093'],
                    max_batch_size=50000,
                    max_batch_time_ms=2000,
                    queued_max_message_kbytes=0,
                    queued_min_messages=1,
                ),
                message_topic=KafkaTopicSettings(
                    name='events2',
                    consumer_group='snuba_consumers2',
                ),
                commit_log_topic='snuba_commit_log',
                replacement_topic='event-replacements',
            )
        )

        assert expected == events_config

    def test_load_external_verride(self):
        DATASETS['events']['consumer']['kafka_cluster_override'] = {
            "queued_max_message_kbytes": 50000,  # ensure 0 works
            "queued_min_messages": 20000,
        }

        events_config = load_dataset_settings(
            dataset='events',
            override={
                'consumer': {
                    'kafka_cluster_override': {
                        'brokers': ['somewhere_else:9093'],
                        'max_batch_size': 10,
                        'max_batch_time_ms': 11,
                    },
                    'message_topic': {
                        'name': 'events3',
                        'consumer_group': 'snuba_consumers3',
                    },
                    'commit_log_topic': 'snuba_commit_log2',
                    'replacement_topic': 'event-replacements2',
                }
            },
        )
        expected = DatasetSettings(
            consumer=DatasetConsumerSettings(
                kafka_cluster=KafkaClusterSettings(
                    brokers=['somewhere_else:9093'],
                    max_batch_size=10,
                    max_batch_time_ms=11,
                    queued_max_message_kbytes=50000,
                    queued_min_messages=20000,
                ),
                message_topic=KafkaTopicSettings(
                    name='events3',
                    consumer_group='snuba_consumers3',
                ),
                commit_log_topic='snuba_commit_log2',
                replacement_topic='event-replacements2',
            )
        )

        assert expected == events_config
