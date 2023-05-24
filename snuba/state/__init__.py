from __future__ import absolute_import, annotations

import logging
import os
import time
from dataclasses import dataclass
from functools import partial
from typing import (
    Any,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    SupportsFloat,
    Tuple,
    Type,
)

import simplejson as json
from confluent_kafka import KafkaError
from confluent_kafka import Message as KafkaMessage
from confluent_kafka import Producer
from sentry_kafka_schemas.schema_types import snuba_queries_v1

from snuba import environment, settings
from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic

metrics = MetricsWrapper(environment.metrics, "snuba.state")
logger = logging.getLogger("snuba.state")

kfk: Producer | None = None
rds = get_redis_client(RedisClientKey.CONFIG)

ratelimit_prefix = "snuba-ratelimit:"
config_hash = "snuba-config"
config_description_hash = "snuba-config-description"
config_history_hash = "snuba-config-history"
config_changes_list = "snuba-config-changes"
config_changes_list_limit = 25
queries_list = "snuba-queries"
rate_limit_config_key = "snuba-ratelimit-config:"

# Rate Limiting and Deduplication

# Window for concurrent query counting
max_query_duration_s = 60
# Window for determining query rate
rate_lookback_s = 60


def _kafka_producer() -> Producer:
    global kfk
    if kfk is None:
        kfk = Producer(
            build_kafka_producer_configuration(
                topic=Topic.QUERYLOG,
                override_params={
                    # at time of writing (2022-05-09) lz4 was chosen because it
                    # compresses quickly. If more compression is needed at the cost of
                    # performance, zstd can be used instead. Recording the query
                    # is part of the API request, therefore speed is important
                    # perf-testing: https://indico.fnal.gov/event/16264/contributions/36466/attachments/22610/28037/Zstd__LZ4.pdf
                    # by default a topic is configured to use whatever compression method the producer used
                    # https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html#topicconfigs_compression.type
                    "compression.type": "lz4",
                },
            )
        )
    return kfk


@dataclass(frozen=True)
class MismatchedTypeException(Exception):
    key: str
    original_type: Type[Any]
    new_type: Type[Any]


class ConfigKeyCallable(Protocol):  # Necessary for typing the memoize
    def __call__(self, config_key: str = config_hash) -> Any:
        pass


class memoize:
    """
    Simple expiring memoizer for state functions that takes a config key.
    """

    def __init__(self, timeout: int = 1) -> None:
        self.timeout = timeout
        self.saved: dict[str, Any] = {}
        self.at: dict[str, float] = {}

    def __call__(self, func: ConfigKeyCallable) -> ConfigKeyCallable:
        def wrapper(config_key: str = config_hash) -> Any:
            now = time.time()
            at = self.at.get(config_key, 0.0)
            if now > at + self.timeout or config_key not in self.saved:
                self.saved[config_key], self.at[config_key] = (
                    func(config_key=config_key),
                    now,
                )
            return self.saved[config_key]

        return wrapper


def get_typed_value(value: Any) -> Any:
    # Return the given value based on its correct type
    # It supports the following types: int, float, string
    if value is None:
        return None
    try:
        assert isinstance(value, (str, int))
        return int(value)
    except (ValueError, AssertionError):
        try:
            assert isinstance(value, (str, SupportsFloat))
            return float(value)
        except (ValueError, AssertionError):
            return value


def set_config(
    key: str,
    value: Optional[Any],
    user: Optional[str] = None,
    force: bool = False,
    config_key: str = config_hash,
) -> None:
    value = get_typed_value(value)
    enc_value = "{}".format(value).encode("utf-8") if value is not None else None
    try:
        enc_original_value = rds.hget(config_key, key)
        if enc_original_value is not None and value is not None:
            original_value = get_typed_value(enc_original_value.decode("utf-8"))
            if value == original_value and type(value) == type(original_value):
                return

            if not force and type(value) != type(original_value):
                raise MismatchedTypeException(key, type(original_value), type(value))

        change_record = (time.time(), user, enc_original_value, enc_value)
        p = rds.pipeline()
        if value is None:
            p.hdel(config_key, key)
            p.hdel(config_history_hash, key)
        else:
            p.hset(config_key, key, enc_value)
            p.hset(config_history_hash, key, json.dumps(change_record))
        p.lpush(config_changes_list, json.dumps((key, change_record)))
        p.ltrim(config_changes_list, 0, config_changes_list_limit)
        p.execute()
        logger.info(f"Successfully changed option {key} to {value}")
    except MismatchedTypeException as exc:
        logger.exception(
            f"Mismatched types for {exc.key}: Original type: {exc.original_type}, New type: {exc.new_type}"
        )
        raise exc
    except Exception as ex:
        logger.exception(ex)


def set_configs(
    values: Mapping[str, Optional[Any]],
    user: Optional[str] = None,
    force: bool = False,
    config_key: str = config_hash,
) -> None:
    for k, v in values.items():
        set_config(k, v, user=user, force=force, config_key=config_key)


def get_config(
    key: str, default: Optional[Any] = None, config_key: str = config_hash
) -> Optional[Any]:
    return get_all_configs(config_key=config_key).get(key, default)


def get_configs(
    key_defaults: Iterable[Tuple[str, Optional[Any]]], config_key: str = config_hash
) -> Sequence[Optional[Any]]:
    all_confs = get_all_configs(config_key=config_key)
    return [all_confs.get(k, d) for k, d in key_defaults]


def get_all_configs(config_key: str = config_hash) -> Mapping[str, Optional[Any]]:
    return {k: v for k, v in get_raw_configs(config_key=config_key).items()}


@memoize(settings.CONFIG_MEMOIZE_TIMEOUT)
def get_raw_configs(config_key: str = config_hash) -> Mapping[str, Optional[Any]]:
    try:
        all_configs = rds.hgetall(config_key)
        configs = {
            k.decode("utf-8"): get_typed_value(v.decode("utf-8"))
            for k, v in all_configs.items()
            if v is not None
        }
        if os.environ.get("SENTRY_SINGLE_TENANT"):
            # Single Tenant has this overriding CONFIG_STATE.
            for k, v in settings.CONFIG_STATE.items():
                configs[k] = v
        return configs
    except Exception as ex:
        logger.exception(ex)
        if os.environ.get("SENTRY_SINGLE_TENANT"):
            return settings.CONFIG_STATE
        return {}


def delete_config(
    key: str, user: Optional[Any] = None, config_key: str = config_hash
) -> None:
    set_config(key, None, user=user, config_key=config_key)


def get_uncached_config(key: str, config_key: str = config_hash) -> Optional[Any]:
    value = rds.hget(config_key, key.encode("utf-8"))
    if value is not None:
        return get_typed_value(value.decode("utf-8"))
    return None


def get_config_changes_legacy() -> Sequence[Any]:
    return [json.loads(change) for change in rds.lrange(config_changes_list, 0, -1)]


def get_config_changes() -> Sequence[Tuple[str, float, Optional[str], Any, Any]]:
    """
    Like get_config_changes_legacy() but ensures that values are cast to their correct type
    """
    changes = get_config_changes_legacy()

    return [
        (key, ts, user, get_typed_value(before), get_typed_value(after))
        for [key, [ts, user, before, after]] in changes
    ]


# Config descriptions for runtime config UI


def set_config_description(
    key: str, description: Optional[str] = None, user: Optional[str] = None
) -> None:
    enc_desc = (
        "{}".format(description).encode("utf-8") if description is not None else None
    )

    try:
        enc_original_desc = rds.hget(config_description_hash, key)

        if (
            enc_original_desc is not None
            and description is not None
            and enc_original_desc.decode("utf-8") == description
        ):
            return

        if description is None:
            rds.hdel(config_description_hash, key)
            logger.info(f"Successfully deleted config description for {key}")
        else:
            rds.hset(config_description_hash, key, enc_desc)
            logger.info(
                f"Successfully changed config description for {key} to '{description}'"
            )

    except Exception as e:
        logger.exception(e)


def get_config_description(key: str) -> Optional[str]:
    try:
        enc_desc = rds.hget(config_description_hash, key)
        return enc_desc.decode("utf-8") if enc_desc is not None else None
    except Exception as e:
        logger.exception(e)
        return None


def get_all_config_descriptions() -> Mapping[str, Optional[str]]:
    try:
        all_descriptions = rds.hgetall(config_description_hash)
        return {
            k.decode("utf-8"): d.decode("utf-8")
            for k, d in all_descriptions.items()
            if d is not None
        }
    except Exception as e:
        logger.exception(e)
        return {}


def delete_config_description(key: str, user: Optional[str] = None) -> None:
    set_config_description(key, None, user=user)


# Query Recording


def safe_dumps_default(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {**value}
    raise TypeError(
        f"Cannot convert object of type {type(value).__name__} to JSON-safe type"
    )


safe_dumps = partial(json.dumps, for_json=True, default=safe_dumps_default)


def _record_query_delivery_callback(
    error: Optional[KafkaError], message: KafkaMessage
) -> None:
    metrics.increment(
        "record_query.delivery_callback",
        tags={"status": "success" if error is None else "failure"},
    )

    if error is not None:
        logger.warning("Could not record query due to error: %r", error)


def record_query(query_metadata: snuba_queries_v1.Querylog) -> None:
    max_redis_queries = 200
    try:
        producer = _kafka_producer()
        data = safe_dumps(query_metadata)
        rds.pipeline(transaction=False).lpush(queries_list, data).ltrim(
            queries_list, 0, max_redis_queries - 1
        ).execute()
        producer.poll(0)  # trigger queued delivery callbacks
        producer.produce(
            settings.KAFKA_TOPIC_MAP.get(Topic.QUERYLOG.value, Topic.QUERYLOG.value),
            data.encode("utf-8"),
            on_delivery=_record_query_delivery_callback,
        )
    except Exception as ex:
        logger.exception("Could not record query due to error: %r", ex)


def flush_producer() -> None:
    global kfk
    if kfk is not None:
        messages_remaining = kfk.flush()
        logger.debug(f"{messages_remaining} querylog messages pending delivery")
