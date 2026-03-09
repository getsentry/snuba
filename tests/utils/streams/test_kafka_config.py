import importlib

from snuba import settings
from snuba.utils.streams.configuration_builder import (
    _get_default_topic_configuration,
    get_default_kafka_configuration,
)
from snuba.utils.streams.topics import Topic


def teardown_function() -> None:
    importlib.reload(settings)


def test_default_config() -> None:
    broker_config = get_default_kafka_configuration()
    assert broker_config["bootstrap.servers"] == settings.BROKER_CONFIG["bootstrap.servers"]


def test_default_config_cli_bootstrap_servers() -> None:
    broker_config = get_default_kafka_configuration(bootstrap_servers=["cli.server:9092"])
    assert broker_config["bootstrap.servers"] == "cli.server:9092"
    broker_config = get_default_kafka_configuration(
        bootstrap_servers=["cli.server:9092", "cli2.server:9092"]
    )
    assert broker_config["bootstrap.servers"] == "cli.server:9092,cli2.server:9092"


def test_default_config_override_new_config() -> None:
    default_broker = "my.broker:9092"
    default_broker_config = {
        "bootstrap.servers": default_broker,
    }
    settings.BROKER_CONFIG = default_broker_config
    broker_config = get_default_kafka_configuration()
    assert broker_config["bootstrap.servers"] == default_broker


def test_kafka_broker_config() -> None:
    default_broker = "my.broker:9092"
    events_broker = "my.other.broker:9092"
    settings.BROKER_CONFIG = {
        "bootstrap.servers": default_broker,
    }

    settings.KAFKA_BROKER_CONFIG = {Topic.EVENTS.value: {"bootstrap.servers": events_broker}}

    events_broker_config = get_default_kafka_configuration(Topic.EVENTS)
    assert events_broker_config["bootstrap.servers"] == events_broker

    other_broker_config = get_default_kafka_configuration(Topic.EVENT_REPLACEMENTS)
    assert other_broker_config["bootstrap.servers"] == default_broker


def test_get_default_topic_configuration() -> None:
    default_broker = "my.broker:9092"
    events_broker = "my.events.broker:9092"
    transactions_broker = "my.transactions.broker:9092"
    sliced_broker = "my.sliced.broker:90992"
    settings.BROKER_CONFIG = {
        "bootstrap.servers": default_broker,
    }

    settings.KAFKA_BROKER_CONFIG = {Topic.EVENTS.value: {"bootstrap.servers": events_broker}}
    settings.SLICED_KAFKA_BROKER_CONFIG = {
        (Topic.TRANSACTIONS.value, 0): {"bootstrap.servers": transactions_broker},
        (Topic.TRANSACTIONS.value, 1): {"bootstrap.servers": sliced_broker},
    }

    slice_1_config = _get_default_topic_configuration(Topic.TRANSACTIONS, 1)
    assert slice_1_config["bootstrap.servers"] == sliced_broker

    slice_0_config = _get_default_topic_configuration(Topic.TRANSACTIONS, 0)
    assert slice_0_config["bootstrap.servers"] == transactions_broker

    events_config = _get_default_topic_configuration(Topic.EVENTS)
    assert events_config["bootstrap.servers"] == events_broker

    default_config = _get_default_topic_configuration(None)
    assert default_config["bootstrap.servers"] == default_broker
