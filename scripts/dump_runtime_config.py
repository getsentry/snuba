#!/usr/bin/env python3
"""Dump snuba runtime-config hashes from the CONFIG Redis.

Runtime config is stored as Redis hashes:
  * ``snuba-config``       -- global runtime config
  * ``capman``             -- all allocation-policy configs
  * ``BaseRoutingStrategy``-- storage-routing strategy configs
  (other ConfigurableComponent namespaces use a hash named after the namespace)

Field keys look like ``{resource}.{ClassName}.{config}[.{param}:{value},...]``.
Parameterized configs escape any '.'/','/':' inside a param name or value as
``__dot_literal__`` / ``__comma_literal__`` / ``__colon_literal__``. This script
decodes those sequences back to real '.'/','/':' so the keys are readable.

Usage:
    uv run python scripts/dump_runtime_config.py                 # capman + snuba-config
    uv run python scripts/dump_runtime_config.py capman          # a specific hash
    uv run python scripts/dump_runtime_config.py --all           # every config hash found
"""
from __future__ import annotations

import argparse
import json
import sys

from snuba.redis import RedisClientKey, get_redis_client
from snuba.state import get_typed_value

# Reverse of ConfigurableComponent._KEY_DELIMITERS_TO_ESCAPE_SEQUENCES.
_ESCAPE_SEQUENCES_TO_DELIMITERS = {
    "__dot_literal__": ".",
    "__comma_literal__": ",",
    "__colon_literal__": ":",
}

# Hashes that hold config values (NOT descriptions / history / change-log).
DEFAULT_HASHES = ["capman", "snuba-config"]

# Redis hashes used by the runtime-config machinery that are not config values
# themselves; skipped by --all.
_NON_CONFIG_HASHES = {
    "snuba-config-description",
    "snuba-config-history",
}


def unescape_key(key: str) -> str:
    """Decode the delimiter-escape sequences back to real '.'/','/':'."""
    for sequence, delimiter in _ESCAPE_SEQUENCES_TO_DELIMITERS.items():
        key = key.replace(sequence, delimiter)
    return key


def dump_hash(client, hash_name: str) -> dict[str, object]:
    """Return {decoded_key: typed_value} for one Redis hash."""
    raw = client.hgetall(hash_name)
    result: dict[str, object] = {}
    for raw_key, raw_value in raw.items():
        key = unescape_key(raw_key.decode("utf-8"))
        value = get_typed_value(raw_value.decode("utf-8")) if raw_value is not None else None
        result[key] = value
    return result


def discover_config_hashes(client) -> list[str]:
    """SCAN for hash keys that look like config stores (excludes helper hashes)."""
    found = []
    for raw_key in client.scan_iter(match="*", count=500):
        name = raw_key.decode("utf-8")
        if name in _NON_CONFIG_HASHES:
            continue
        try:
            if client.type(raw_key) == b"hash":
                found.append(name)
        except Exception:
            continue
    return sorted(found)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "hashes",
        nargs="*",
        help=f"hash name(s) to dump (default: {', '.join(DEFAULT_HASHES)})",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="discover and dump every hash in the CONFIG Redis (excludes description/history)",
    )
    args = parser.parse_args()

    client = get_redis_client(RedisClientKey.CONFIG)

    if args.all:
        hashes = discover_config_hashes(client)
    else:
        hashes = args.hashes or DEFAULT_HASHES

    output = {name: dump_hash(client, name) for name in hashes}
    # drop empty hashes so the dump only shows what's actually set
    output = {name: values for name, values in output.items() if values}

    json.dump(output, sys.stdout, indent=2, sort_keys=True, default=str)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
