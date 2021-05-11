from enum import Enum
from typing import Mapping


# These are the default topic names, they can be changed via settings
class Topic(Enum):
    EVENTS = "events"
    EVENT_REPLACEMENTS = "event-replacements"
    EVENT_REPLACEMENTS_LEGACY = "event-replacements-legacy"
    COMMIT_LOG = "snuba-commit-log"
    CDC = "cdc"
    OUTCOMES = "outcomes"
    SESSIONS = "ingest-sessions"
    SUBSCRIPTION_RESULTS_EVENTS = "events-subscription-results"
    SUBSCRIPTION_RESULTS_TRANSACTIONS = "transactions-subscription-results"
    QUERYLOG = "snuba-queries"


def get_topic_creation_config(topic: Topic) -> Mapping[str, str]:
    config = {Topic.EVENTS: {"message.timestamp.type": "LogAppendTime"}}
    return config.get(topic, {})
