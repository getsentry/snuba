import logging

from typing import Any, Mapping


def _validate_settings(locals: Mapping[str, Any]) -> None:
    logger = logging.getLogger("snuba.settings")

    if locals["QUERIES_TOPIC"] != "snuba-queries":
        raise ValueError("QUERIES_TOPIC is deprecated. Use KAFKA_TOPIC_MAP instead.")

    if locals["STORAGE_TOPICS"]:
        logger.warning(
            "DEPRECATED: STORAGE_TOPICS is deprecated. Use KAFKA_TOPIC_MAP instead."
        )

    if locals.get("STORAGE_BROKER_CONFIG"):
        logger.warning(
            "DEPRECATED: STORAGE_BROKER_CONFIG is deprecated. Use KAFKA_BROKER_CONFIG instead."
        )

    topic_names = {
        "events",
        "event-replacements",
        "snuba-commit-log",
        "cdc",
        "outcomes",
        "ingest-sessions",
        "snuba-queries",
    }

    for key in locals["KAFKA_TOPIC_MAP"].keys():
        if key not in topic_names:
            raise ValueError(f"Invalid topic value: {key}")

    for key in locals["KAFKA_BROKER_CONFIG"].keys():
        if key not in topic_names:
            raise ValueError(f"Invalid topic value {key}")
