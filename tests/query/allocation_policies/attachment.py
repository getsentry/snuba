from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sentry_options.testing import override_options

from snuba.query.allocation_policies import ALLOCATION_POLICY_KEY


def match_block(policies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"match": {}, "policies": policies}]


# Today's live EAP attachment (was hardcoded on StorageRouting).
CURRENT_EAP_ATTACHMENT: list[dict[str, Any]] = match_block(
    [
        {
            "name": "ConcurrentQueriesPerOrgPolicy",
            "limit": 66,
            "enforce": "never",
        },
    ]
)


@contextmanager
def override_allocation_policy(
    attachment: dict[str, Any],
) -> Iterator[None]:
    with override_options("snuba", {ALLOCATION_POLICY_KEY: attachment}):
        yield


@contextmanager
def override_current_eap_policies() -> Iterator[None]:
    """Attach today's EAP policy list and the settings the hardcoded ctor used to inject."""
    with override_allocation_policy({"EAP": CURRENT_EAP_ATTACHMENT}):
        yield
