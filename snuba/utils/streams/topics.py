from __future__ import annotations

from typing import Any, Iterator, Mapping

# These are the default topic names, they can be changed via settings
_HARDCODED_TOPICS = {
    "EVENTS": "events",
    "EVENT_REPLACEMENTS": "event-replacements",
    "SNUBA_COMMIT_LOG": "snuba-commit-log",
    "CDC": "cdc",
    "TRANSACTIONS": "transactions",
    "SNUBA_TRANSACTIONS_COMMIT_LOG": "snuba-transactions-commit-log",
    "SNUBA_METRICS": "snuba-metrics",
    "OUTCOMES": "outcomes",
    "INGEST_SESSIONS": "ingest-sessions",
    "SNUBA_METRICS_COMMIT_LOG": "snuba-metrics-commit-log",
    "SCHEDULED_SUBSCRIPTIONS_EVENTS": "scheduled-subscriptions-events",
    "SCHEDULED_SUBSCRIPTIONS_TRANSACTIONS": "scheduled-subscriptions-transactions",
    "SCHEDULED_SUBSCRIPTIONS_METRICS": "scheduled-subscriptions-metrics",
    "SCHEDULED_SUBSCRIPTIONS_GENERIC_METRICS_SETS": "scheduled-subscriptions-generic-metrics-sets",
    "SCHEDULED_SUBSCRIPTIONS_GENERIC_METRICS_DISTRIBUTIONS": "scheduled-subscriptions-generic-metrics-distributions",
    "EVENTS_SUBSCRIPTION_RESULTS": "events-subscription-results",
    "TRANSACTIONS_SUBSCRIPTION_RESULTS": "transactions-subscription-results",
    "METRICS_SUBSCRIPTION_RESULTS": "metrics-subscription-results",
    "GENERIC_METRICS_SETS_SUBSCRIPTION_RESULTS": "generic-metrics-sets-subscription-results",
    "GENERIC_METRICS_DISTRIBUTIONS_SUBSCRIPTION_RESULTS": "generic-metrics-distributions-subscription-results",
    "SNUBA_QUERIES": "snuba-queries",
    "PROCESSED_PROFILES": "processed-profiles",
    "PROFILES_CALL_TREE": "profiles-call-tree",
    "INGEST_REPLAY_EVENTS": "ingest-replay-events",
    "SNUBA_GENERIC_METRICS": "snuba-generic-metrics",
    "SNUBA_GENERIC_METRICS_SETS_COMMIT_LOG": "snuba-generic-metrics-sets-commit-log",
    "SNUBA_GENERIC_METRICS_DISTRIBUTIONS_COMMIT_LOG": "snuba-generic-metrics-distributions-commit-log",
    "SNUBA_DEAD_LETTER_INSERTS": "snuba-dead-letter-inserts",
    "SNUBA_ATTRIBUTION": "snuba-attribution",
    "SNUBA_DEAD_LETTER_METRICS": "snuba-dead-letter-metrics",
    "SNUBA_DEAD_LETTER_SESSIONS": "snuba-dead-letter-sessions",
    "SNUBA_DEAD_LETTER_GENERIC_METRICS": "snuba-dead-letter-generic-metrics",
    "SNUBA_DEAD_LETTER_REPLAYS": "snuba-dead-letter-replays",
    "SNUBA_SESSIONS_COMMIT_LOG": "snuba-sessions-commit-log",
    "SCHEDULED_SUBSCRIPTIONS_SESSIONS": "scheduled-subscriptions-sessions",
    "SESSIONS_SUBSCRIPTIONS_RESULTS": "sessions-subscription-results",
    "SNUBA_REPLAY_EVENTS": "snuba-replay-events",
}

_REGISTERED_TOPICS: dict[str, str] = {}


class _Topic(type):
    def __getattr__(self, attr: str) -> "Topic":
        if attr not in _HARDCODED_TOPICS and attr not in _REGISTERED_TOPICS:
            raise AttributeError(attr)
        return Topic(attr.lower().replace("_", "-"))

    def __iter__(self) -> Iterator[Topic]:
        return iter(
            Topic(value)
            for value in {**_HARDCODED_TOPICS, **_REGISTERED_TOPICS}.values()
        )


class Topic(metaclass=_Topic):
    def __init__(self, value: str):
        self.value = value

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Topic) and other.value == self.value

    def __repr__(self) -> str:
        return f"Topic.{self.value.upper()}"


def register_topic(key: str) -> Topic:
    _REGISTERED_TOPICS[key.upper().replace("-", "_")] = key.lower()
    return Topic(key)


def get_topic_creation_config(topic: Topic) -> Mapping[str, str]:
    config = {
        Topic.EVENTS: {"message.timestamp.type": "LogAppendTime"},
        Topic.TRANSACTIONS: {"message.timestamp.type": "LogAppendTime"},
        Topic.SNUBA_METRICS: {"message.timestamp.type": "LogAppendTime"},
        Topic.PROCESSED_PROFILES: {"message.timestamp.type": "LogAppendTime"},
        Topic.INGEST_REPLAY_EVENTS: {"message.timestamp.type": "LogAppendTime"},
        Topic.SNUBA_GENERIC_METRICS: {"message.timestamp.type": "LogAppendTime"},
    }
    return config.get(topic, {})
