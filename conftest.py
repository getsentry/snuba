import hashlib
import os
import sys

import pytest

os.environ.setdefault("SNUBA_SETTINGS", "test")
os.environ.setdefault("CLICKHOUSE_DATABASE", "snuba_test")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # Optional CI sharding: split the collected tests across N runners by a stable
    # hash of the node id. Inert unless SNUBA_TEST_SHARD_TOTAL > 1 is set.
    total = int(os.environ.get("SNUBA_TEST_SHARD_TOTAL", "1"))
    shard = int(os.environ.get("SNUBA_TEST_SHARD", "0"))
    print(f"[shard-config] SNUBA_TEST_SHARD={shard} SNUBA_TEST_SHARD_TOTAL={total}", file=sys.stderr)
    if total <= 1:
        return
    selected: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        digest = int(hashlib.md5(item.nodeid.encode()).hexdigest(), 16)
        (selected if digest % total == shard else deselected).append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)
