import importlib
from snuba import settings

from snuba.datasets.storages import StorageKey
from snuba.utils.streams.backends.kafka import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic


def teardown_function() -> None:
    importlib.reload(settings)


def test_default_config():
    broker_config = get_default_kafka_configuration()
    assert (
        broker_config["bootstrap.servers"]
        == settings.BROKER_CONFIG["bootstrap.servers"]
    )


def test_default_config_cli_bootstrap_servers():
    broker_config = get_default_kafka_configuration(
        bootstrap_servers=["cli.server:9092"]
    )
    assert broker_config["bootstrap.servers"] == "cli.server:9092"
    broker_config = get_default_kafka_configuration(
        bootstrap_servers=["cli.server:9092", "cli2.server:9092"]
    )
    assert broker_config["bootstrap.servers"] == "cli.server:9092,cli2.server:9092"


def test_default_config_legacy_override_storage_servers():
    storage_name = StorageKey.EVENTS.value
    storage_key = StorageKey(storage_name)
    default_broker = "my.broker:9092"
    settings.DEFAULT_STORAGE_BROKERS = {storage_name: [default_broker]}
    broker_config = get_default_kafka_configuration(
        storage_key=storage_key, topic=Topic.EVENTS
    )
    assert broker_config["bootstrap.servers"] == default_broker

    default_brokers = ["my.broker:9092", "my.second.broker:9092"]
    settings.DEFAULT_STORAGE_BROKERS = {storage_name: default_brokers}
    broker_config = get_default_kafka_configuration(
        storage_key=storage_key, topic=Topic.EVENTS
    )
    assert broker_config["bootstrap.servers"] == ",".join(default_brokers)


def test_default_config_legacy_override_storage_servers_fallback():
    default_broker = "my.other.broker:9092"
    default_brokers = ["my.broker:9092", "my.second.broker:9092"]
    settings.BROKER_CONFIG = {"bootstrap.servers": default_broker}
    settings.DEFAULT_STORAGE_BROKERS = {
        StorageKey.EVENTS.value: default_brokers,
    }
    storage_key = StorageKey(StorageKey.ERRORS)
    broker_config = get_default_kafka_configuration(
        storage_key=storage_key, topic=Topic.EVENTS
    )
    assert broker_config["bootstrap.servers"] == default_broker


def test_default_config_override_new_config():
    default_broker = "my.broker:9092"
    default_broker_config = {
        "bootstrap.servers": default_broker,
    }
    settings.BROKER_CONFIG = default_broker_config
    broker_config = get_default_kafka_configuration()
    assert broker_config["bootstrap.servers"] == default_broker


def test_default_config_override_new_config_storage():
    default_broker = "my.other.broker:9092"
    default_broker_config = {
        "bootstrap.servers": default_broker,
    }
    settings.STORAGE_BROKER_CONFIG = {
        StorageKey.EVENTS.value: default_broker_config,
    }
    broker_config = get_default_kafka_configuration(StorageKey.EVENTS, Topic.EVENTS)
    assert broker_config["bootstrap.servers"] == default_broker

    other_broker = "another.broker:9092"
    settings.BROKER_CONFIG = {
        "bootstrap.servers": other_broker,
    }
    broker_config = get_default_kafka_configuration(StorageKey.ERRORS, Topic.EVENTS)
    assert broker_config["bootstrap.servers"] == other_broker


def test_default_config_new_fallback_old():
    old_default_broker = "my.broker:9092"
    default_broker = "my.other.broker:9092"
    default_broker_config = {
        "bootstrap.servers": default_broker,
    }
    settings.BROKER_CONFIG = {"bootstrap.servers": old_default_broker}
    settings.STORAGE_BROKER_CONFIG = {
        StorageKey.EVENTS.value: default_broker_config,
    }
    broker_config = get_default_kafka_configuration(StorageKey.ERRORS, Topic.EVENTS)
    assert broker_config["bootstrap.servers"] == old_default_broker


def test_default_config_new_fallback_old_storage():
    old_default_broker = "my.broker:9092"
    default_broker = "my.other.broker:9092"
    default_broker_config = {
        "bootstrap.servers": default_broker,
    }
    settings.BROKER_CONFIG = default_broker_config
    settings.STORAGE_BROKER_CONFIG = {}
    settings.DEFAULT_STORAGE_BROKERS = {
        StorageKey.ERRORS.value: [old_default_broker],
    }
    broker_config = get_default_kafka_configuration(StorageKey.ERRORS, Topic.EVENTS)
    assert broker_config["bootstrap.servers"] == old_default_broker
