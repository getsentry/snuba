from datetime import datetime
from typing import Dict, List, Tuple

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
    Outcome,
    OutcomeCategory,
)
from tests.helpers import write_raw_unprocessed_events


def gen_ingest_outcome(
    time: datetime,
    num: int,
    project_id: int = 1,
    org_id: int = 1,
    outcome_category: int = OutcomeCategory.SPAN_INDEXED,
) -> Dict[str, int | str | None]:
    """Generate a single ingest outcome record.

    Args:
        time: The timestamp for the outcome
        num: The number of outcomes/quantity
        project_id: The project ID (defaults to 1)
        org_id: The organization ID (defaults to 1)
        outcome_category: The outcome category (defaults to SPAN_INDEXED)

    Returns:
        A dictionary representing an outcome record
    """
    return {
        "org_id": project_id,
        "project_id": org_id,
        "key_id": None,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "outcome": Outcome.ACCEPTED,
        "reason": None,
        "event_id": None,
        "quantity": num,
        "category": outcome_category,
    }


def store_outcomes_data(
    outcome_data: List[Tuple[datetime, int]],
    outcome_category: int = OutcomeCategory.SPAN_INDEXED,
) -> None:
    """Store outcomes data to the outcomes storage.

    Args:
        outcome_data: List of tuples containing either:
            - (datetime, number_of_outcomes) - uses outcome_category parameter
        outcome_category: The outcome category to use for all records when using 2-tuple format
                         (defaults to SPAN_INDEXED, ignored for 3-tuple format)
    """
    outcomes_storage = get_storage(StorageKey("outcomes_raw"))
    messages = []

    for item in outcome_data:
        if len(item) == 2:
            time, num_outcomes = item
            category = outcome_category
        else:
            raise ValueError(f"Invalid tuple length: {len(item)}. Expected 2 or 3 elements.")

        messages.append(gen_ingest_outcome(time, num_outcomes, outcome_category=category))

    write_raw_unprocessed_events(outcomes_storage, messages)  # type: ignore


# Available outcome categories (from OutcomeCategory class):
# - OutcomeCategory.SPAN_INDEXED = 16 (for spans)
# - OutcomeCategory.LOG_ITEM = 23 (for logs)

# Usage examples:
# 1. Store span outcomes (default):
#    store_outcomes_data([(datetime1, 1000), (datetime2, 2000)])
#
# 2. Store log outcomes:
#    store_outcomes_data([(datetime1, 1000), (datetime2, 2000)], OutcomeCategory.LOG_ITEM)
#
# 3. Store mixed outcomes:
#    store_outcomes_data([
#        (datetime1, 1000, OutcomeCategory.SPAN_INDEXED),
#        (datetime2, 2000, OutcomeCategory.LOG_ITEM)
#    ])

# Backward compatibility alias
gen_span_ingest_outcome = gen_ingest_outcome

# Re-export for convenience
__all__ = [
    "store_outcomes_data",
    "gen_ingest_outcome",
    "gen_span_ingest_outcome",
    "OutcomeCategory",
    "Outcome",
]
