from __future__ import annotations

from typing import Callable

from flask import request

from snuba import settings
from snuba.admin.jwt import AdminUser, validate_assertion


class UnauthorizedException(Exception):
    pass


auth_provider = Callable[[], AdminUser]

# This function takes the Flask request and authorizes it.
# If the request is valid it would return the user id.
# If not it will raise UnauthorizedException
#
# TODO: provide a more structured representation of the User that
# includes the role at least.
def authorize_request() -> AdminUser:
    provider_id = settings.ADMIN_AUTH_PROVIDER
    provider = AUTH_PROVIDERS.get(provider_id)
    if provider is None:
        raise ValueError("Invalid authorization provider")
    return provider()


def passthrough_authorize() -> AdminUser:
    return AdminUser(email="unknown", id="unknown")


def iap_authorize() -> AdminUser:
    assertion = request.headers.get("X-Goog-IAP-JWT-Assertion")

    if assertion is None:
        raise UnauthorizedException("no JWT present in request headers")

    return validate_assertion(assertion)


AUTH_PROVIDERS = {
    "NOOP": passthrough_authorize,
    "IAP": iap_authorize,
}
