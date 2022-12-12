from typing import Any, Dict, Mapping, MutableMapping, Set, Tuple

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

        sliced_logical_topic = topic_tuple[0]
        assert (
            sliced_logical_topic not in locals["KAFKA_BROKER_CONFIG"]
        ), f"logical topic {sliced_logical_topic} is sliced. It is defined in SLICED_KAFKA_TOPIC_MAP and should only be in SLICED_KAFKA_BROKER_CONFIG, not KAFKA_BROKER_CONFIG"

    for topic_tuple in locals["SLICED_KAFKA_BROKER_CONFIG"]:
        logical_topic = topic_tuple[0]

        assert (
            logical_topic not in locals["KAFKA_TOPIC_MAP"]
        ), f"logical topic {logical_topic} is not sliced. It is defined in KAFKA_TOPIC_MAP and should only be in KAFKA_BROKER_CONFIG, not SLICED_KAFKA_BROKER_CONFIG"

    _STORAGE_SET_CLUSTER_MAP: Dict[str, Mapping[str, Any]] = {}

    _SLICED_STORAGE_SET_CLUSTER_MAP: Dict[Tuple[str, int], Mapping[str, Any]] = {}
    for cluster in locals["CLUSTERS"]:
        for storage_set in cluster["storage_sets"]:
            _STORAGE_SET_CLUSTER_MAP[storage_set] = cluster

    for sliced_cluster in locals["SLICED_CLUSTERS"]:
        for storage_set_tuple in sliced_cluster["storage_set_slices"]:
            _SLICED_STORAGE_SET_CLUSTER_MAP[
                (storage_set_tuple[0], storage_set_tuple[1])
            ] = sliced_cluster

    for storage_set in locals["SLICED_STORAGE_SETS"]:
        num_slices = locals["SLICED_STORAGE_SETS"][storage_set]

        for slice_id in range(num_slices):
            assert (
                storage_set,
                slice_id,
            ) in _SLICED_STORAGE_SET_CLUSTER_MAP, f"storage set, slice id pair ({storage_set}, {slice_id}) is not assigned any cluster in SLICED_CLUSTERS in settings"

    all_storage_set_keys = set()
    all_storage_set_keys = set(_STORAGE_SET_CLUSTER_MAP.keys()).union(
        {key[0] for key in _SLICED_STORAGE_SET_CLUSTER_MAP.keys()}
    )

    for storage_set_key in all_storage_set_keys:
        single_node_vals: Set[bool] = set()
        single_node_vals.add(_STORAGE_SET_CLUSTER_MAP[storage_set_key]["single_node"])

        for storage_set_tuple in _SLICED_STORAGE_SET_CLUSTER_MAP.keys():
            if storage_set_key in storage_set_tuple:
                single_node_vals.add(
                    _SLICED_STORAGE_SET_CLUSTER_MAP[storage_set_tuple]["single_node"]
                )

        assert (
            len(single_node_vals) == 1
        ), f"Storage set key {storage_set_key} must have the same single_node value for all of its associated clusters"
