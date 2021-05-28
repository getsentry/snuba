import copy
import logging
from typing import Any, Dict, Mapping, Optional, Sequence

from snuba.utils.logging import pylog_to_syslog_level

logger = logging.getLogger(__name__)

KafkaBrokerConfig = Dict[str, Any]


DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 50000
DEFAULT_QUEUED_MIN_MESSAGES = 10000
DEFAULT_PARTITIONER = "consistent"
DEFAULT_MAX_MESSAGE_BYTES = 50000000  # 50MB, default is 1MB
SUPPORTED_KAFKA_CONFIGURATION = (
    # Check https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    # for the full list of available options
    "bootstrap.servers",
    "sasl.mechanism",
    "sasl.username",
    "sasl.password",
    "security.protocol",
    "ssl.ca.location",
    "ssl.ca.certificate.stores",
    "ssl.certificate.location",
    "ssl.certificate.pem",
    "ssl.cipher.suites",
    "ssl.crl.location",
    "ssl.curves.list",
    "ssl.endpoint.identification.algorithm",
    "ssl.key.location",
    "ssl.key.password",
    "ssl.key.pem",
    "ssl.keystore.location",
    "ssl.keystore.password",
    "ssl.sigalgs.list",
)


def build_kafka_configuration_with_overrides(
    default_config: Mapping[str, Any],
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
) -> KafkaBrokerConfig:
    default_bootstrap_servers = None
    broker_config = copy.deepcopy(default_config)
    assert isinstance(broker_config, dict)
    bootstrap_servers = (
        ",".join(bootstrap_servers) if bootstrap_servers else default_bootstrap_servers
    )
    if bootstrap_servers:
        broker_config["bootstrap.servers"] = bootstrap_servers
    broker_config = {k: v for k, v in broker_config.items() if v is not None}
    for configuration_key in broker_config:
        if configuration_key not in SUPPORTED_KAFKA_CONFIGURATION:
            raise ValueError(
                f"The `{configuration_key}` configuration key is not supported."
            )

    broker_config["log_level"] = pylog_to_syslog_level(logger.getEffectiveLevel())

    if override_params:
        broker_config.update(override_params)

    return broker_config


def build_kafka_consumer_configuration_with_overrides(
    default_config: Mapping[str, Any],
    group_id: str,
    auto_offset_reset: str = "error",
    queued_max_messages_kbytes: int = DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
    queued_min_messages: int = DEFAULT_QUEUED_MIN_MESSAGES,
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
) -> KafkaBrokerConfig:

    broker_config = build_kafka_configuration_with_overrides(
        default_config, bootstrap_servers, override_params
    )

    broker_config.update(
        {
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            # overridden to reduce memory usage when there's a large backlog
            "queued.max.messages.kbytes": queued_max_messages_kbytes,
            "queued.min.messages": queued_min_messages,
            "enable.partition.eof": False,
        }
    )
    return broker_config
