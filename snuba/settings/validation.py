from typing import Any, Mapping, MutableMapping

from snuba.datasets.slicing import SENTRY_LOGICAL_PARTITIONS


class InvalidTopicError(ValueError):
    pass


slice_count_validation_msg = """physical slice for storage set {0}'s logical partition {1} is {2},
            but only {3} physical slices are assigned to {0}"""


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
        "scheduled-subscriptions-generic-metrics-counters",
        "events-subscription-results",
        "transactions-subscription-results",
        "sessions-subscription-results",
        "metrics-subscription-results",
        "generic-metrics-subscription-results",
        "processed-profiles",
        "snuba-attribution",
        "profiles-call-tree",
        "ingest-replay-events",
        "generic-events",
        "snuba-generic-events-commit-log",
        "snuba-dead-letter-replays",
        "snuba-generic-metrics",
        "snuba-generic-metrics-sets-commit-log",
        "snuba-generic-metrics-distributions-commit-log",
        "snuba-generic-metrics-counters-commit-log",
        "snuba-dead-letter-generic-metrics",
        "snuba-dead-letter-sessions",
        "snuba-dead-letter-metrics",
        "snuba-dead-letter-metrics-sets",
        "snuba-dead-letter-metrics-distributions",
        "snuba-dead-letter-metrics-counters",
        "snuba-dead-letter-generic-events",
        "snuba-dead-letter-querylog",
        "snuba-generic-events-commit-log",
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


def validate_slicing_settings(locals: Mapping[str, Any]) -> None:
    for storage_set in locals["SLICED_STORAGE_SETS"]:
        assert (
            storage_set in locals["LOGICAL_PARTITION_MAPPING"]
        ), "sliced mapping must be defined for sliced storage set {storage_set}"

        storage_set_mapping = locals["LOGICAL_PARTITION_MAPPING"][storage_set]
        defined_slice_count = locals["SLICED_STORAGE_SETS"][storage_set]

        for logical_part in range(0, SENTRY_LOGICAL_PARTITIONS):
            slice_id = storage_set_mapping.get(logical_part)

            assert (
                slice_id is not None
            ), f"missing physical slice for storage set {storage_set}'s logical partition {logical_part}"

            assert (
                slice_id >= 0 and slice_id < defined_slice_count
            ), slice_count_validation_msg.format(
                storage_set, logical_part, slice_id, defined_slice_count
            )

    for topic_tuple in locals["SLICED_KAFKA_TOPIC_MAP"]:
        assert (
            topic_tuple in locals["SLICED_KAFKA_BROKER_CONFIG"]
        ), f"missing broker config definition for sliced Kafka topic {topic_tuple[0]} on slice {topic_tuple[1]}"
