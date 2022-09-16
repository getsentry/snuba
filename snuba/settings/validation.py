from typing import Any, Mapping, MutableMapping

from snuba.datasets.partitioning import SENTRY_LOGICAL_PARTITIONS


class InvalidTopicError(ValueError):
    pass


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
        "ingest-replay-events",
        "snuba-replay-events",
        "snuba-dead-letter-replays",
        "snuba-generic-metrics",
        "snuba-generic-metrics-sets-commit-log",
        "snuba-generic-metrics-distributions-commit-log",
        "snuba-dead-letter-generic-metrics",
        "snuba-dead-letter-sessions",
        "snuba-dead-letter-metrics",
    }

    for key in locals["KAFKA_TOPIC_MAP"].keys():
        if key not in topic_names:
            raise InvalidTopicError(f"Invalid topic value: {key}")

    for key in locals["KAFKA_BROKER_CONFIG"].keys():
        if key not in topic_names:
            raise ValueError(f"Invalid topic value {key}")

    # Validate cluster configuration
    from snuba.clusters.storage_sets import StorageSetKey

    storage_set_to_cluster: MutableMapping[StorageSetKey, Any] = {}

    for cluster in locals["CLUSTERS"]:
        for cluster_storage_set in cluster["storage_sets"]:
            try:
                storage_set_to_cluster[StorageSetKey(cluster_storage_set)] = cluster
            except ValueError:
                # We allow definition of storage_sets in configuration files
                # that are not defined in StorageSetKey.
                pass

    for logical_part in range(0, SENTRY_LOGICAL_PARTITIONS):
        physical_part = locals["LOGICAL_PARTITION_MAPPING"].get(str(logical_part))

        assert (
            physical_part is not None
        ), f"missing physical partition for logical partition {logical_part}"
