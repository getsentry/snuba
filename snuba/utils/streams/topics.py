from enum import Enum
from typing import Mapping


# These are the default topic names, they can be changed via settings
class Topic(Enum):
    EVENTS = "events"
    EVENT_REPLACEMENTS = "event-replacements"
    COMMIT_LOG = "snuba-commit-log"
    CDC = "cdc"
    TRANSACTIONS = "transactions"
    TRANSACTIONS_COMMIT_LOG = "snuba-transactions-commit-log"
    METRICS = "snuba-metrics"
    OUTCOMES = "outcomes"
    SESSIONS = "ingest-sessions"
    METRICS_COMMIT_LOG = "snuba-metrics-commit-log"
    SUBSCRIPTION_SCHEDULED_EVENTS = "scheduled-subscriptions-events"
    SUBSCRIPTION_SCHEDULED_TRANSACTIONS = "scheduled-subscriptions-transactions"
    SUBSCRIPTION_SCHEDULED_METRICS = "scheduled-subscriptions-metrics"
    SUBSCRIPTION_RESULTS_EVENTS = "events-subscription-results"
    SUBSCRIPTION_RESULTS_TRANSACTIONS = "transactions-subscription-results"
    SUBSCRIPTION_RESULTS_METRICS = "metrics-subscription-results"
    QUERYLOG = "snuba-queries"
    PROFILES = "processed-profiles"
    DEAD_LETTER_QUEUE_INSERTS = "snuba-dead-letter-inserts"
    DEAD_LETTER_METRICS = "snuba-dead-letter-metrics"
    DEAD_LETTER_SESSIONS = "snuba-dead-letter-sessions"
    EXPERIMENTAL = "experimental"


def get_topic_creation_config(topic: Topic) -> Mapping[str, str]:
    config = {
        Topic.EVENTS: {"message.timestamp.type": "LogAppendTime"},
        Topic.TRANSACTIONS: {"message.timestamp.type": "LogAppendTime"},
        Topic.METRICS: {"message.timestamp.type": "LogAppendTime"},
        Topic.PROFILES: {"message.timestamp.type": "LogAppendTime"},
    }
    return config.get(topic, {})
