from collections.abc import Iterator

import pytest

from tests.query.allocation_policies.attachment import override_current_eap_policies


@pytest.fixture(autouse=True)
def _attach_current_eap_policies() -> Iterator[None]:
    """RPC tests expect today's EAP policy list + ctor settings, now sourced from options."""
    with override_current_eap_policies():
        yield
