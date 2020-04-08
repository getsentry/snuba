from typing import Mapping, Set


STORAGE_SETS: Mapping[str, Set[str]] = {
    "events": {"errors", "events", "groupedmessages"},
    "groupassignees": {"groupassignees"},
    "outcomes": {"outcomes_raw", "outcomes_hourly"},
    "querylog": {"querylog"},
    "sessions": {"sessions_raw", "sessions_hourly"},
    "transactions": {"transactions"},
}
