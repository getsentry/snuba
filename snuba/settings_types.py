import collections
import copy

KafkaClusterSettings = collections.namedtuple(
    'KafkaClusterSettings',
    'brokers max_batch_size max_batch_time_ms queued_max_message_kbytes queued_min_messages',
)

KafkaTopicSettings = collections.namedtuple(
    'KafkaTopicSettings',
    'name consumer_group',
)

DatasetConsumerSettings = collections.namedtuple(
    'DatasetConsumerSettings',
    'kafka_cluster message_topic commit_log_topic replacement_topic',
)

DatasetSettings = collections.namedtuple(
    'DatasetSettings',
    'consumer'
)


def deep_copy_and_merge(default, override, skip_null=False):
    """
    Return a deep copy of 'default' dictionary after merging 'override'
    into it.
    This is meant to define overrides in settings.py file
    """
    base = copy.deepcopy(default)

    def deep_merge(base, override, skip_null=False):
        for key, value in override.items():
            if isinstance(value, dict):
                node = base.setdefault(key, {})
                deep_merge(node, value, skip_null)
            else:
                if not skip_null or value is not None:
                    base[key] = value

        return base

    return deep_merge(base, override, skip_null)


def recursively_remove_none(config):
    return deep_copy_and_merge(
        default={},
        override=config,
        skip_null=True,
    )
