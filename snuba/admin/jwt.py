from typing import Any, Optional

import requests

from snuba import settings
from snuba.admin.user import AdminUser

CERTS: Optional[Any] = None


def _certs() -> Any:
    """Returns a dictionary of current Google public key certificates for
    validating Google-signed JWTs. Since these change rarely, the result
    is cached on first request for faster subsequent responses.
    """

    global CERTS
    if CERTS is None:
        response = requests.get("https://www.gstatic.com/iap/verify/public_key")
        CERTS = response.json()
    return CERTS


def _audience() -> str:
    return settings.ADMIN_AUTH_JWT_AUDIENCE


def validate_assertion(assertion: str) -> AdminUser:
    """
    Checks that the JWT assertion is valid (properly signed, for the
    correct audience) and if so, returns an AdminUser.

    If not, an exception will be raised
    """
    from jose import jwt

    info = jwt.decode(assertion, _certs(), algorithms=["ES256"], audience=_audience())
    return AdminUser(email=info["email"], id=info["sub"])
