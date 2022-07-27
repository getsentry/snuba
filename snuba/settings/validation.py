from typing import Any, Mapping, MutableMapping


def validate_settings(locals: Mapping[str, Any]) -> None:
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
        "transactions",
        "snuba-commit-log",
        "snuba-transactions-commit-log",
        "snuba-sessions-commit-log",
        "snuba-metrics-commit-log",
        "cdc",
        "snuba-metrics",
        "outcomes",
        "ingest-sessions",
        "snuba-queries",
        "scheduled-subscriptions-events",
        "scheduled-subscriptions-transactions",
        "scheduled-subscriptions-sessions",
        "scheduled-subscriptions-metrics",
        "scheduled-subscriptions-generic-metrics-sets",
        "scheduled-subscriptions-generic-metrics-distributions",
        "events-subscription-results",
        "transactions-subscription-results",
        "sessions-subscription-results",
        "metrics-subscription-results",
        "generic-metrics-sets-subscription-results",
        "generic-metrics-distributions-subscription-results",
        "snuba-dead-letter-inserts",
        "processed-profiles",
        "snuba-attribution",
        "profiles-call-tree",
        "snuba-replay-events",
        "ingest-replay-events",
        "snuba-generic-metrics",
        "snuba-generic-metrics-sets-commit-log",
        "snuba-generic-metrics-distributions-commit-log",
        "snuba-dead-letter-generic-metrics",
        "snuba-dead-letter-sessions",
        "snuba-dead-letter-metrics",
        "snuba-dead-letter-replays",
    }

    for key in locals["KAFKA_TOPIC_MAP"].keys():
        if key not in topic_names:
            raise ValueError(f"Invalid topic value: {key}")

    for key in locals["KAFKA_BROKER_CONFIG"].keys():
        if key not in topic_names:
            raise ValueError(f"Invalid topic value {key}")

    # Validate cluster configuration
    from snuba.clusters.storage_sets import JOINABLE_STORAGE_SETS, StorageSetKey

    storage_set_to_cluster: MutableMapping[StorageSetKey, Any] = {}

    for cluster in locals["CLUSTERS"]:
        for cluster_storage_set in cluster["storage_sets"]:
            try:
                storage_set_to_cluster[StorageSetKey(cluster_storage_set)] = cluster
            except ValueError:
                # We allow definition of storage_sets in configuration files
                # that are not defined in StorageSetKey.
                pass

    for group in JOINABLE_STORAGE_SETS:
        clusters = [storage_set_to_cluster[storage_set] for storage_set in group]

        first = clusters[0]
        for cluster in clusters[1:]:
            if first != cluster:
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
                    assert first.get(property) == cluster.get(
                        property
                    ), f"Invalid property: {property}"
