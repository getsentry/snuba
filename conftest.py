import hashlib
import os

os.environ.setdefault("SNUBA_SETTINGS", "test")
os.environ.setdefault("CLICKHOUSE_DATABASE", "snuba_test")


def pytest_collection_modifyitems(config, items):
    # Optional CI sharding: split the collected tests across N runners by a stable
    # hash of the node id. Inert unless SNUBA_TEST_SHARD_TOTAL > 1 is set.
    total = int(os.environ.get("SNUBA_TEST_SHARD_TOTAL", "1"))
    if total <= 1:
        return
    shard = int(os.environ.get("SNUBA_TEST_SHARD", "0"))
    selected, deselected = [], []
    for item in items:
        digest = int(hashlib.md5(item.nodeid.encode()).hexdigest(), 16)
        (selected if digest % total == shard else deselected).append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)
