import re

from snuba.clickhouse.pool import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey

ClickhouseVersion = tuple[int, int]

# SAMPLE BY <identifier> or SAMPLE BY <fn>(...)
_SAMPLE_BY_RE = re.compile(
    r"\s+SAMPLE BY\s+\S+(?:\([^)]*\))?",
    re.IGNORECASE,
)


def strip_sample_by_clause(create_table_statement: str) -> str:
    """Remove a SAMPLE BY clause from a ClickHouse SHOW CREATE TABLE statement."""
    return _SAMPLE_BY_RE.sub("", create_table_statement, count=1)


def replace_create_table_name(create_table_statement: str, old_name: str, new_name: str) -> str:
    """Rename the table in a CREATE TABLE statement without rewriting ZooKeeper paths."""
    pattern = re.compile(
        rf"(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)((?:[`\w]+\.)?`?){re.escape(old_name)}(`?)",
        re.IGNORECASE,
    )
    replaced, count = pattern.subn(rf"\g<1>\g<2>{new_name}\g<3>", create_table_statement, count=1)
    if count != 1:
        raise ValueError(f"Could not rename {old_name!r} to {new_name!r} in CREATE TABLE statement")
    return replaced


def get_clickhouse_version_for_storage_set(
    storage_set: StorageSetKey, clickhouse: ClickhousePool | None
) -> ClickhouseVersion:
    """
    Determine the clickhouse version for a storage set. Assumes (and verifies)
    that all local nodes have the same version for simplicity.
    """

    if clickhouse is not None:
        connections = [clickhouse]
    else:
        cluster = get_cluster(storage_set)
        connections = [
            cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
            for node in cluster.get_local_nodes()
        ]

    versions: set[ClickhouseVersion] = set()

    for connection in connections:
        ver = connection.execute("SELECT version()").results[0][0]

        major, minor, *_ = ver.split(".")
        versions.add((int(major), int(minor)))

    if len(versions) != 1:
        raise RuntimeError(
            f"found multiple clickhouse versions in local nodes of storage set {storage_set}: {versions}"
        )

    return versions.pop()


_CLICKHOUSE_SETTINGS_SUPPORTED = {
    # https://github.com/ClickHouse/ClickHouse/pull/12433#issuecomment-685987783
    "allow_nullable_key": (20, 7),
}


def supports_setting(clickhouse_version: ClickhouseVersion, setting: str) -> bool:
    """
    For a given setting, determine whether the given clickhouse version
    supports it.
    """
    return _CLICKHOUSE_SETTINGS_SUPPORTED[setting] <= clickhouse_version
