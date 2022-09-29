from dataclasses import dataclass
from typing import Any, Optional

import structlog

logger = structlog.get_logger().bind(module=__name__)


CERTS: Optional[Any] = None
AUDIENCE: Optional[str] = None


@dataclass
class AdminUser:
    email: str
    id: str


def certs() -> Any:
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


def get_metadata(item_name: str) -> str:
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


def audience() -> str:
    """Returns the audience value (the JWT 'aud' property) for the current
    running instance. Since this involves a metadata lookup, the result is
    cached when first requested for faster future responses.
    """
    global AUDIENCE
    if AUDIENCE is None:
        project_number = get_metadata("numeric-project-id")
        project_id = get_metadata("project-id")
        AUDIENCE = "/projects/{}/apps/{}".format(project_number, project_id)
    return AUDIENCE


def validate_assertion(assertion: str) -> AdminUser:
    """Checks that the JWT assertion is valid (properly signed, for the
    correct audience) and if so, returns strings for the requesting user's
    email and a persistent user ID. If not valid, returns None for each field.
    """
    from jose import jwt

    info = jwt.decode(assertion, certs(), algorithms=["ES256"], audience=audience())
    return AdminUser(email=info["email"], id=info["id"])
