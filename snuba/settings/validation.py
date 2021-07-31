from typing import Any, Mapping, MutableMapping


def _validate_settings(locals: Mapping[str, Any]) -> None:
    if locals.get("QUERIES_TOPIC"):
        raise ValueError("QUERIES_TOPIC is deprecated. Use KAFKA_TOPIC_MAP instead.")

    if locals.get("STORAGE_TOPICS"):
        raise ValueError("STORAGE_TOPICS is deprecated. Use KAFKA_TOPIC_MAP instead.")

    if locals.get("STORAGE_BROKER_CONFIG"):
        raise ValueError(
            "DEPRECATED: STORAGE_BROKER_CONFIG is deprecated. Use KAFKA_BROKER_CONFIG instead."
        )

    if locals.get("DEFAULT_STORAGE_BROKERS"):
        raise ValueError(
            "DEFAULT_STORAGE_BROKERS is deprecated. Use KAFKA_BROKER_CONFIG instead."
        )

    topic_names = {
        "events",
        "event-replacements",
        "event-replacements-legacy",
        "snuba-commit-log",
        "cdc",
        "ingest-metrics",
        "outcomes",
        "ingest-sessions",
        "snuba-queries",
        "events-subscription-results",
        "transactions-subscription-results",
    }

    for key in locals["KAFKA_TOPIC_MAP"].keys():
        if key not in topic_names:
            raise ValueError(f"Invalid topic value: {key}")

    for key in locals["KAFKA_BROKER_CONFIG"].keys():
        if key not in topic_names:
            raise ValueError(f"Invalid topic value {key}")

    # Validate cluster configuration
    from snuba.clusters.storage_sets import STORAGE_SET_GROUPS, StorageSetKey

    storage_set_to_cluster: MutableMapping[StorageSetKey, Any] = {}

    for cluster in locals["CLUSTERS"]:
        for cluster_storage_set in cluster["storage_sets"]:
            storage_set_to_cluster[StorageSetKey(cluster_storage_set)] = cluster

    for group in STORAGE_SET_GROUPS:
        first = storage_set_to_cluster[group[0]]
        for storage_set in group[1:]:
            current = storage_set_to_cluster[storage_set]
            if first != current:
                for property in [
                    "host",
                    "port",
                    "user",
                    "password",
                    "database",
                    "http_port",
                    "single_node",
                    "distributed_cluster_name",
                ]:
                    assert first.get(property) == current.get(
                        property
                    ), f"Invalid {property}"
