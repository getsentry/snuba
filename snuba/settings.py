from snuba.settings_types import (
    KafkaClusterSettings,
    KafkaTopicSettings,
    DatasetConsumerSettings,
    DatasetSettings,
    deep_copy_and_merge,
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


def load_dataset_settings(dataset, override={}):
    global DATASETS
    global KAFKA_CLUSTERS
    raw_config = DATASETS[dataset]
    raw_config = deep_copy_and_merge(raw_config, override)

    consumer_config = raw_config['consumer']

    # per dataset override
    kafka_cluster_name = consumer_config['kafka_cluster_base']
    kafka_cluster_config = KAFKA_CLUSTERS[kafka_cluster_name]
    kafka_cluster = deep_copy_and_merge(
        kafka_cluster_config,
        consumer_config['kafka_cluster_override'],
    )

    return DatasetSettings(
        consumer=DatasetConsumerSettings(
            kafka_cluster=KafkaClusterSettings(
                brokers=kafka_cluster['brokers'],
                max_batch_size=kafka_cluster['max_batch_size'],
                max_batch_time_ms=kafka_cluster['max_batch_time_ms'],
                queued_max_message_kbytes=kafka_cluster['queued_max_message_kbytes'],
                queued_min_messages=kafka_cluster['queued_min_messages'],
            ),
            message_topic=KafkaTopicSettings(
                name=consumer_config['message_topic']['name'],
                consumer_group=consumer_config['message_topic']['consumer_group'],
            ),
            commit_log_topic=consumer_config['commit_log_topic'],
            replacement_topic=consumer_config['replacement_topic'],
        )
    )
