from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any

from sentry_options.testing import override_options
from sentry_relay.consts import DataCategory

from snuba.configs.configuration import (
    CONFIGURABLE_COMPONENT_OVERRIDES_KEY,
    ConfigurableComponent,
)
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.storage_routing.routing_strategies.common import Outcome
from tests.helpers import write_raw_unprocessed_events


def override_component_config(
    component: ConfigurableComponent,
    config_key: str,
    value: Any,
    params: dict[str, Any] | None = None,
) -> AbstractContextManager[None]:
    """Set a ConfigurableComponent config via the ``configurable_component_overrides``
    sentry-option for the duration of the context.

    Routing strategies no longer fall back to the legacy Redis runtime config, so
    ``set_config_value`` writes are not read back by ``get_config_value``; tests
    must supply overrides through this option instead. The key is built with the
    component's own key builder so it matches exactly what ``get_config_value``
    looks up, and the value is stored as a number just like production data.
    """
    full_key = component._build_runtime_config_key(config_key, params or {})
    return override_options("snuba", {CONFIGURABLE_COMPONENT_OVERRIDES_KEY: {full_key: value}})


def gen_ingest_outcome(
    time: datetime,
    num: int,
    project_id: int = 1,
    org_id: int = 1,
    outcome_category: int = DataCategory.SPAN_INDEXED,
) -> dict[str, int | str | None]:
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
        "org_id": org_id,
        "project_id": project_id,
        "key_id": None,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "outcome": Outcome.ACCEPTED,
        "reason": None,
        "event_id": None,
        "quantity": num,
        "category": outcome_category,
    }


def store_outcomes_data(
    outcome_data: list[tuple[datetime, int]],
    outcome_category: int = DataCategory.SPAN_INDEXED,
    org_id: int = 1,
    project_id: int = 1,
) -> None:
    """Store outcomes data to the outcomes storage.

    Args:
        outcome_data: List of tuples containing either:
            - (datetime, number_of_outcomes) - uses outcome_category parameter
        outcome_category: The outcome category to use for all records when using 2-tuple format
                         (defaults to SPAN_INDEXED, ignored for 3-tuple format)
    """
    outcomes_storage = get_writable_storage(StorageKey("outcomes_raw"))
    messages = []

    for item in outcome_data:
        if len(item) == 2:
            time, num_outcomes = item
            category = outcome_category
        else:
            raise ValueError(f"Invalid tuple length: {len(item)}. Expected 2 or 3 elements.")

        messages.append(
            gen_ingest_outcome(
                time, num_outcomes, outcome_category=category, org_id=org_id, project_id=project_id
            )
        )
    write_raw_unprocessed_events(outcomes_storage, messages)


# Available outcome categories (from DataCategory class):
# - DataCategory.SPAN_INDEXED = 16 (for spans)
# - DataCategory.LOG_ITEM = 23 (for logs)

# Usage examples:
# 1. Store span outcomes (default):
#    store_outcomes_data([(datetime1, 1000), (datetime2, 2000)])
#
# 2. Store log outcomes:
#    store_outcomes_data([(datetime1, 1000), (datetime2, 2000)], DataCategory.LOG_ITEM)
#
# 3. Store mixed outcomes:
#    store_outcomes_data([
#        (datetime1, 1000, DataCategory.SPAN_INDEXED),
#        (datetime2, 2000, DataCategory.LOG_ITEM)
#    ])

# Backward compatibility alias
gen_span_ingest_outcome = gen_ingest_outcome

# Re-export for convenience
__all__ = [
    "store_outcomes_data",
    "gen_ingest_outcome",
    "gen_span_ingest_outcome",
    "override_component_config",
    "DataCategory",
    "Outcome",
]
