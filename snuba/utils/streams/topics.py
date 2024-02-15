from enum import Enum
from typing import Mapping

from sentry_kafka_schemas import SchemaNotFound, get_topic


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
    SESSIONS_COMMIT_LOG = "snuba-sessions-commit-log"
    METRICS_COMMIT_LOG = "snuba-metrics-commit-log"
    SUBSCRIPTION_SCHEDULED_EVENTS = "scheduled-subscriptions-events"
    SUBSCRIPTION_SCHEDULED_TRANSACTIONS = "scheduled-subscriptions-transactions"
    SUBSCRIPTION_SCHEDULED_SESSIONS = "scheduled-subscriptions-sessions"
    SUBSCRIPTION_SCHEDULED_METRICS = "scheduled-subscriptions-metrics"
    SUBSCRIPTION_SCHEDULED_GENERIC_METRICS_SETS = (
        "scheduled-subscriptions-generic-metrics-sets"
    )
    SUBSCRIPTION_SCHEDULED_GENERIC_METRICS_DISTRIBUTIONS = (
        "scheduled-subscriptions-generic-metrics-distributions"
    )
    SUBSCRIPTION_SCHEDULED_GENERIC_METRICS_COUNTERS = (
        "scheduled-subscriptions-generic-metrics-counters"
    )

    SUBSCRIPTION_RESULTS_EVENTS = "events-subscription-results"
    SUBSCRIPTION_RESULTS_TRANSACTIONS = "transactions-subscription-results"
    SUBSCRIPTION_RESULTS_SESSIONS = "sessions-subscription-results"
    SUBSCRIPTION_RESULTS_METRICS = "metrics-subscription-results"

    SUBSCRIPTION_RESULTS_GENERIC_METRICS = "generic-metrics-subscription-results"

    QUERYLOG = "snuba-queries"
    PROFILES = "processed-profiles"
    PROFILES_FUNCTIONS = "profiles-call-tree"
    REPLAYEVENTS = "ingest-replay-events"
    GENERIC_METRICS = "snuba-generic-metrics"
    GENERIC_METRICS_SETS_COMMIT_LOG = "snuba-generic-metrics-sets-commit-log"
    GENERIC_METRICS_DISTRIBUTIONS_COMMIT_LOG = (
        "snuba-generic-metrics-distributions-commit-log"
    )
    GENERIC_METRICS_COUNTERS_COMMIT_LOG = "snuba-generic-metrics-counters-commit-log"
    GENERIC_EVENTS = "generic-events"
    GENERIC_EVENTS_COMMIT_LOG = "snuba-generic-events-commit-log"
    GROUP_ATTRIBUTES = "group-attributes"

    ATTRIBUTION = "snuba-attribution"
    DEAD_LETTER_METRICS = "snuba-dead-letter-metrics"
    DEAD_LETTER_SESSIONS = "snuba-dead-letter-sessions"
    DEAD_LETTER_GENERIC_METRICS = "snuba-dead-letter-generic-metrics"
    DEAD_LETTER_REPLAYS = "snuba-dead-letter-replays"
    DEAD_LETTER_GENERIC_EVENTS = "snuba-dead-letter-generic-events"
    DEAD_LETTER_QUERYLOG = "snuba-dead-letter-querylog"
    DEAD_LETTER_GROUP_ATTRIBUTES = "snuba-dead-letter-group-attributes"

    SPANS = "snuba-spans"
    METRICS_SUMMARIES = "snuba-metrics-summaries"

    COGS_SHARED_RESOURCES_USAGE = "shared-resources-usage"


def get_topic_creation_config(topic: Topic) -> Mapping[str, str]:
    try:
        return get_topic(topic.value)["topic_creation_config"]
    # TODO: Remove this once all topics needed by snuba are registered in sentry-kafka-schemas
    except SchemaNotFound:
        return {}
