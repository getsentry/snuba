import re

NESTED_COL_EXPR_RE = re.compile(r"^([a-zA-Z0-9_\.]+)\[([a-zA-Z0-9_\.:-]+)\]$")


#: Metrics granularities for which a materialized view exist, in ascending order
GRANULARITIES_AVAILABLE = (10, 60, 60 * 60, 24 * 60 * 60)

# Number of EAP Span buckets
# Changing this column will change Snuba's understanding of how attributes
# on spans are bucketed into different columns
# This will affect migrations and querying.
ATTRIBUTE_BUCKETS = 20

# Maximum number of attempts to fetch profile events
PROFILE_EVENTS_MAX_ATTEMPTS = (
    4  # Will result in ~23 seconds total wait time with exponential backoff
)

# Maximum wait time between attempts in seconds
PROFILE_EVENTS_MAX_WAIT_SECONDS = 16
