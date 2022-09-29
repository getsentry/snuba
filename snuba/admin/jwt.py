from dataclasses import dataclass
from typing import Any, Optional

import structlog

logger = structlog.get_logger().bind(module=__name__)


CERTS: Optional[Any] = None
AUDIENCE: Optional[str] = None


@dataclass
class AdminUser:
    """
    Basic encapsulation of a user of the admin panel. In the future,
    should be extended to contain permissions among other things
    """

    email: str
    id: str


def _certs() -> Any:
    """Returns a dictionary of current Google public key certificates for
    validating Google-signed JWTs. Since these change rarely, the result
    is cached on first request for faster subsequent responses.
    """
    import requests

    global CERTS
    if CERTS is None:
        response = requests.get("https://www.gstatic.com/iap/verify/public_key")
        CERTS = response.json()
    return CERTS


def _get_metadata(item_name: str) -> str:
    """Returns a string with the project metadata value for the item_name.
    See https://cloud.google.com/compute/docs/storing-retrieving-metadata for
    possible item_name values.
    """
    import requests

    endpoint = "http://metadata.google.internal"
    path = "/computeMetadata/v1/project/"
    path += item_name
    response = requests.get(
        "{}{}".format(endpoint, path), headers={"Metadata-Flavor": "Google"}
    )
    metadata = response.text
    return metadata


def _audience() -> str:
    """Returns the audience value (the JWT 'aud' property) for the current
    running instance. Since this involves a metadata lookup, the result is
    cached when first requested for faster future responses.
    """
    global AUDIENCE
    if AUDIENCE is None:
        project_number = _get_metadata("numeric-project-id")
        project_id = _get_metadata("project-id")
        AUDIENCE = "/projects/{}/apps/{}".format(project_number, project_id)
    return AUDIENCE


def validate_assertion(assertion: str) -> AdminUser:
    """
    Checks that the JWT assertion is valid (properly signed, for the
    correct audience) and if so, returns an AdminUser.

    If not, an exception will be raised
    """
    from jose import jwt

    info = jwt.decode(assertion, _certs(), algorithms=["ES256"], audience=_audience())
    return AdminUser(email=info["email"], id=info["id"])
