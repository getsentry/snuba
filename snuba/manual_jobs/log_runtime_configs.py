import base64
import json
from typing import Any

from snuba.manual_jobs import Job, JobLogger
from snuba.redis import RedisClientKey, RedisClientType, get_redis_client

PAYLOAD_START_MARKER = "===== BEGIN CONFIG DUMP ====="
PAYLOAD_END_MARKER = "===== END CONFIG DUMP ====="


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


def _read_value(client: RedisClientType, key: str) -> Any:
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


class LogRuntimeConfigs(Job):
    """Dumps the Snuba config Redis keys as a single JSON payload that can be
    pasted into an LLM to help migrate config to sentry-options (see
    getsentry/snuba#8168).

    Only the specific config keys are read (never a blind scan): the config
    client can be the shared default Redis, so scanning it would leak unrelated
    keys (cache, admin roles, job logs, ...) into the logs. The dumped keys are
    the runtime config and the allocation-policy / routing-strategy overrides
    (``capman`` / ``cbrs`` hashes, keyed exactly like the
    ``configurable_component_overrides`` sentry-option).

    Run it repeatably straight from the CLI (no job manifest entry, no
    job-status guard) with ``snuba jobs dump_runtime_configs``.
    """

    allow_adhoc_run = True

    def _config_keys(self) -> list[str]:
        # Imported lazily so pulling the RPC/routing stack (via storage_routing)
        # doesn't happen for every `snuba jobs` invocation -- snuba.manual_jobs
        # eagerly imports all job modules.
        from snuba.query.allocation_policies import CAPMAN_HASH
        from snuba.state import config_hash
        from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
            CBRS_HASH,
        )

        return [
            config_hash,
            CAPMAN_HASH,
            CBRS_HASH,
        ]

    def execute(self, logger: JobLogger) -> None:
        client = get_redis_client(RedisClientKey.CONFIG)
        dump: dict[str, Any] = {}
        for key in self._config_keys():
            if client.exists(key):
                dump[key] = _read_value(client, key)

        logger.info(f"config keys dumped: {sorted(dump.keys())}")
        logger.info(PAYLOAD_START_MARKER)
        logger.info(json.dumps({"config": dump}, indent=2, sort_keys=True, default=str))
        logger.info(PAYLOAD_END_MARKER)
