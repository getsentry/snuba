from snuba.settings_types import (
    KafkaClusterSettings,
    KafkaTopicSettings,
    DatasetConsumerSettings,
    DatasetSettings,
)


def _load_settings(obj=locals()):
    """Load settings from the path provided in the SNUBA_SETTINGS environment
    variable. Defaults to `./snuba/settings_base.py`. Users can provide a
    short name like `test` that will be expanded to `settings_test.py` in the
    main Snuba directory, or they can provide a full absolute path such as
    `/foo/bar/my_settings.py`."""

    import os
    import imp

    path = os.path.dirname(__file__)

    settings = os.environ.get('SNUBA_SETTINGS', 'base')
    if not settings.startswith('/') and not settings.startswith('settings_'):
        settings = 'settings_%s' % settings
    if not settings.endswith('.py'):
        settings += '.py'

    settings = os.path.join(path, settings)
    settings = imp.load_source('snuba.settings', settings)

    for attr in dir(settings):
        if attr.isupper():
            obj[attr] = getattr(settings, attr)


_load_settings()


def _merge_kafka_cluster_config(base, override):
    if not override:
        return base
    return KafkaClusterSettings(
        brokers=override['brokers'] or base['brokers'],
        max_batch_size=override['max_batch_size'] or base['max_batch_size'],
        max_batch_time_ms=override['queued_max_message_kbytes']
        or base['queued_max_message_kbytes'],
        queued_min_messages=override['queued_min_messages']
        or base['queued_min_messages'],
        queued_max_message_kbytes=override['queued_max_message_kbytes']
        or base['queued_max_message_kbytes']
    )


def __deep_merge(default, override):
    for key, value in override.items():
        if isinstance(value, dict):
            node = default[key]
            __deep_merge(node, value)
        else:
            default[key] = value

    return default


def load_dataset_settings(dataset, override={}):
    global DATASETS
    raw_config = DATASETS[dataset]

    override = __deep_merge(
        default={
            'consumer': {
                'kafka_cluster': {
                    'brokers': None,
                    'max_batch_size': None,
                    'max_batch_time_ms': None,
                    'queued_max_message_kbytes': None,
                    'queued_min_messages': None,
                },
                'message_topic': {
                    'name': None,
                    'consumer_group': None,
                },
                'commit_log_topic': None,
                'replacement_topic': None,
            }
        },
        override=override,
    )

    return DatasetSettings(
        consumer=DatasetConsumerSettings(
            kafka_cluster=_merge_kafka_cluster_config(
                _merge_kafka_cluster_config(
                    raw_config['consumer']['kafka_cluster']['base'],
                    raw_config['consumer']['kafka_cluster']['override'],
                ),
                override['consumer']['kafka_cluster'],
            ),
            message_topic=KafkaTopicSettings(
                name=override['consumer']['message_topic']['name']
                or raw_config['consumer']['message_topic']['name'],
                consumer_group=override['consumer']['message_topic']['consumer_group']
                or raw_config['consumer']['message_topic']['consumer_group'],
            ),
            commit_log_topic=override['consumer']['commit_log_topic']
            or raw_config['consumer']['commit_log_topic'],
            replacement_topic=override['consumer']['replacement_topic']
            or raw_config['consumer']['replacement_topic'],
        )
    )
