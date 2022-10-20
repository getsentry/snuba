from __future__ import annotations

from typing import Any, Iterator, Mapping

# These are the default topic names, they can be changed via settings
_HARDCODED_TOPICS = {
    "EVENTS": "events",
    "EVENT_REPLACEMENTS": "event-replacements",
    "COMMIT_LOG": "snuba-commit-log",
    "CDC": "cdc",
    "TRANSACTIONS": "transactions",
    "TRANSACTIONS_COMMIT_LOG": "snuba-transactions-commit-log",
    "METRICS": "snuba-metrics",
    "OUTCOMES": "outcomes",
    "SESSIONS": "ingest-sessions",
    "METRICS_COMMIT_LOG": "snuba-metrics-commit-log",
    "SUBSCRIPTION_SCHEDULED_EVENTS": "scheduled-subscriptions-events",
    "SUBSCRIPTION_SCHEDULED_TRANSACTIONS": "scheduled-subscriptions-transactions",
    "SUBSCRIPTION_SCHEDULED_METRICS": "scheduled-subscriptions-metrics",
    "SUBSCRIPTION_SCHEDULED_GENERIC_METRICS_SETS": "scheduled-subscriptions-generic-metrics-sets",
    "SUBSCRIPTION_SCHEDULED_GENERIC_METRICS_DISTRIBUTIONS": "scheduled-subscriptions-generic-metrics-distributions",
    "SUBSCRIPTION_RESULTS_EVENTS": "events-subscription-results",
    "SUBSCRIPTION_RESULTS_TRANSACTIONS": "transactions-subscription-results",
    "SUBSCRIPTION_RESULTS_METRICS": "metrics-subscription-results",
    "SUBSCRIPTION_RESULTS_GENERIC_METRICS_SETS": "generic-metrics-sets-subscription-results",
    "SUBSCRIPTION_RESULTS_GENERIC_METRICS_DISTRIBUTIONS": "generic-metrics-distributions-subscription-results",
    "QUERYLOG": "snuba-queries",
    "PROFILES": "processed-profiles",
    "PROFILES_FUNCTIONS": "profiles-call-tree",
    "REPLAYEVENTS": "ingest-replay-events",
    "GENERIC_METRICS": "snuba-generic-metrics",
    "GENERIC_METRICS_SETS_COMMIT_LOG": "snuba-generic-metrics-sets-commit-log",
    "GENERIC_METRICS_DISTRIBUTIONS_COMMIT_LOG": "snuba-generic-metrics-distributions-commit-log",
    "DEAD_LETTER_QUEUE_INSERTS": "snuba-dead-letter-inserts",
    "ATTRIBUTION": "snuba-attribution",
    "DEAD_LETTER_METRICS": "snuba-dead-letter-metrics",
    "DEAD_LETTER_SESSIONS": "snuba-dead-letter-sessions",
    "DEAD_LETTER_GENERIC_METRICS": "snuba-dead-letter-generic-metrics",
    "DEAD_LETTER_REPLAYS": "snuba-dead-letter-replays",
}

_REGISTERED_TOPICS: dict[str, str] = {}


class _Topic(type):
    def __getattr__(self, attr: str) -> "Topic":
        if attr not in _HARDCODED_TOPICS and attr not in _REGISTERED_TOPICS:
            raise AttributeError(attr)
        return Topic(attr.lower())

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
    _REGISTERED_TOPICS[key.upper()] = key.lower()
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
