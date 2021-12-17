from __future__ import annotations

from typing import Callable

from snuba import settings


class UnauthorizedException(Exception):
    pass


auth_provider = Callable[[], str]

# This function takes the Flask request and authorizes it.
# If the request is valid it would return the user id.
# If not it will raise UnauthorizedException
#
# TODO: provide a more structured representation of the User that
# includes the role at least.
def authorize_request() -> str:
    provider_id = settings.ADMIN_AUTH_PROVIDER
    provider = AUTH_PROVIDERS.get(provider_id)
    if provider is None:
        raise ValueError("Invalid authorization provider")
    return provider()


def passthrough_authorize() -> str:
    return "unknown"


def iap_authorize() -> str:
    raise NotImplementedError


AUTH_PROVIDERS = {
    "NOOP": passthrough_authorize,
    "IAP": iap_authorize,
}
