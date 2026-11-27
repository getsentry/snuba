from __future__ import annotations

import json
from collections.abc import Sequence

import rapidjson
import structlog
from flask import request

from snuba import settings
from snuba.admin.auth_roles import DEFAULT_ROLES, ROLES
from snuba.admin.google import CloudIdentityAPI
from snuba.admin.jwt import validate_assertion
from snuba.admin.user import AdminUser
from snuba.redis import RedisClientKey, get_redis_client

USER_HEADER_KEY = "X-Goog-Authenticated-User-Email"
SERVICE_ACCOUNT_EMAIL_SUFFIX = ".gserviceaccount.com"

redis_client = get_redis_client(RedisClientKey.ADMIN_AUTH)

logger = structlog.get_logger().bind(module=__name__)


class UnauthorizedException(Exception):
    pass


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

    return _set_roles(provider())


def _is_member_of_group(user: AdminUser, group: str) -> bool:
    google_api = CloudIdentityAPI()
    return google_api.check_group_membership(group_email=group, member=user.email)


def get_iam_roles_from_user(user: AdminUser) -> Sequence[str]:
    iam_roles = []
    try:
        with open(settings.ADMIN_IAM_POLICY_FILE) as policy_file:
            policy = json.load(policy_file)
            for binding in policy["bindings"]:
                role: str = binding["role"].split("roles/")[-1]
                for member in binding["members"]:
                    if member in (f"user:{user.email}", f"serviceAccount:{user.email}"):
                        iam_roles.append(role)
                        break
                    if member.startswith("group:"):
                        group = member.split("group:")[-1]
                        if _is_member_of_group(user, group):
                            iam_roles.append(role)
                            break
    except FileNotFoundError:
        logger.warn(
            f"IAM policy file not found {settings.ADMIN_IAM_POLICY_FILE}. Using default roles only."
        )

    return iam_roles


def get_cached_iam_roles(user: AdminUser) -> Sequence[str]:
    iam_roles_str = redis_client.get(f"roles-{user.email}")
    if not iam_roles_str:
        return []

    iam_roles = rapidjson.loads(iam_roles_str)
    if isinstance(iam_roles, list):
        return iam_roles

    return []


def _set_roles(user: AdminUser) -> AdminUser:
    # todo: depending on provider convert user email
    # to subset of DEFAULT_ROLES based on IAM roles
    iam_roles: Sequence[str] = []
    try:
        iam_roles = get_cached_iam_roles(user)
    except Exception as e:
        logger.exception("Failed to load roles from cache", exception=e)

    if not iam_roles:
        iam_roles = get_iam_roles_from_user(user)
        try:
            redis_client.set(
                f"roles-{user.email}",
                rapidjson.dumps(iam_roles),
                ex=settings.ADMIN_ROLES_REDIS_TTL,
            )
        except Exception as e:
            logger.exception(e)

    roles = [ROLES[role] for role in iam_roles if role in ROLES]
    # A service account bound in the IAM policy gets only its bound roles, not
    # the defaults every person gets, so automation stays least-privilege.
    if not (roles and user.email.endswith(SERVICE_ACCOUNT_EMAIL_SUFFIX)):
        roles.extend(DEFAULT_ROLES)
    user.roles = roles
    return user


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
