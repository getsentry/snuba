import base64
import json
from typing import Any

from snuba.manual_jobs import Job, JobLogger
from snuba.redis import RedisClientKey, RedisClientType, get_redis_client

# Ephemeral / high-churn stores we deliberately skip: the query cache and the
# rate-limiter counters. Everything else we store in Redis is dumped.
_EXCLUDED_CLIENTS = {RedisClientKey.CACHE, RedisClientKey.RATE_LIMITER}

PAYLOAD_START_MARKER = "===== BEGIN REDIS DUMP ====="
PAYLOAD_END_MARKER = "===== END REDIS DUMP ====="


def _decode_key(value: Any) -> str:
    """Redis keys / hash field names must be JSON-object keys, so always
    produce a string (base64-tagged when the bytes are not valid utf-8)."""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return "__base64__:" + base64.b64encode(value).decode("ascii")
    return str(value)


def _decode_value(value: Any) -> Any:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return {"__base64__": base64.b64encode(value).decode("ascii")}
    return value


def _read_value(client: RedisClientType, key: Any) -> Any:
    key_type = _decode_value(client.type(key))
    if key_type == "string":
        return _decode_value(client.get(key))
    if key_type == "hash":
        return {_decode_key(k): _decode_value(v) for k, v in client.hgetall(key).items()}
    if key_type == "list":
        return [_decode_value(v) for v in client.lrange(key, 0, -1)]
    if key_type == "set":
        return sorted((_decode_value(v) for v in client.smembers(key)), key=repr)
    if key_type == "zset":
        return [
            [_decode_value(member), score]
            for member, score in client.zrange(key, 0, -1, withscores=True)
        ]
    return f"<unsupported redis type: {key_type}>"


def _dump_client(client: RedisClientType) -> dict[str, Any]:
    dump: dict[str, Any] = {}
    for raw_key in client.scan_iter(count=1000):
        dump[_decode_key(raw_key)] = _read_value(client, raw_key)
    return dict(sorted(dump.items()))


class LogRuntimeConfigs(Job):
    """Dumps every value we store in Redis (all runtime configs, allocation
    policy / CBRS overrides, and other operational state) as a single JSON
    payload that can be pasted into an LLM to help migrate config to
    sentry-options (see getsentry/snuba#8168).

    The query cache and rate-limiter stores are skipped -- they are ephemeral
    and not worth dumping. Everything else is dumped grouped by Redis client,
    with each key read according to its Redis type. The allocation-policy /
    CBRS overrides live in the ``config`` client under the ``capman`` and
    ``cbrs`` hashes, keyed exactly like the ``configurable_component_overrides``
    sentry-option.

    Run it repeatably straight from the CLI (no job manifest entry, no
    job-status guard) with ``snuba jobs dump_runtime_configs``.
    """

    allow_adhoc_run = True

    def _collect_redis_dump(self, logger: JobLogger) -> dict[str, Any]:
        dumps_by_client_id: dict[int, dict[str, Any]] = {}
        result: dict[str, Any] = {}
        for client_key in RedisClientKey:
            if client_key in _EXCLUDED_CLIENTS:
                continue
            try:
                client = get_redis_client(client_key)
                # Several logical stores can share one physical client; dump
                # each physical client only once.
                client_id = id(client)
                if client_id not in dumps_by_client_id:
                    dumps_by_client_id[client_id] = _dump_client(client)
                result[client_key.value] = dumps_by_client_id[client_id]
            except Exception as e:
                logger.error(f"failed to dump redis client {client_key.value}: {e}")
        return result

    def execute(self, logger: JobLogger) -> None:
        dump = self._collect_redis_dump(logger)
        for client_name, contents in dump.items():
            logger.info(f"redis client {client_name}: {len(contents)} key(s)")
        logger.info(PAYLOAD_START_MARKER)
        logger.info(json.dumps({"redis": dump}, indent=2, sort_keys=True, default=str))
        logger.info(PAYLOAD_END_MARKER)
