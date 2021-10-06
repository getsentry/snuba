from enum import Enum
from typing import Mapping

from snuba import settings


# These are the default topic names, they can be changed via settings
class Topic(Enum):
    EVENTS = "events"
    EVENT_REPLACEMENTS = "event-replacements"
    EVENT_REPLACEMENTS_LEGACY = "event-replacements-legacy"
    COMMIT_LOG = "snuba-commit-log"
    CDC = "cdc"
    METRICS = "snuba-metrics"
    OUTCOMES = "outcomes"
    SESSIONS = "ingest-sessions"
    SESSIONS_COMMIT_LOG = "snuba-sessions-commit-log"
    SUBSCRIPTION_SCHEDULED_EVENTS = "scheduled-subscriptions-events"
    SUBSCRIPTION_SCHEDULED_TRANSACTIONS = "scheduled-subscriptions-transactions"
    SUBSCRIPTION_SCHEDULED_SESSIONS = "scheduled-subscriptions-sessions"
    SUBSCRIPTION_RESULTS_EVENTS = "events-subscription-results"
    SUBSCRIPTION_RESULTS_TRANSACTIONS = "transactions-subscription-results"
    SUBSCRIPTION_RESULTS_SESSIONS = "sessions-subscription-results"
    QUERYLOG = "snuba-queries"


def get_topic_creation_config(topic: Topic) -> Mapping[str, str]:
    config = {
        Topic.EVENTS: {"message.timestamp.type": "LogAppendTime"},
        Topic.METRICS: {"message.timestamp.type": "LogAppendTime"},
    }
    if settings.ENABLE_SESSIONS_SUBSCRIPTIONS:
        config.update({Topic.SESSIONS: {"message.timestamp.type": "LogAppendTime"}})
    return config.get(topic, {})
