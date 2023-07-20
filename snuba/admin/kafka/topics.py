import json
from typing import Any, Sequence

from confluent_kafka.admin import AdminClient

from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic


def get_broker_data() -> Sequence[Any]:
    data = []

    broker_configs = [get_default_kafka_configuration(topic=topic) for topic in Topic]
    seen_broker_configs = set()

    for broker_config in broker_configs:
        broker_config_str = str(broker_config)
        if broker_config_str in seen_broker_configs:
            continue
        seen_broker_configs.add(broker_config_str)
        client = AdminClient(broker_config)
        data.append(json.dumps(list(client.list_topics().topics.values()), default=str))
    return data
