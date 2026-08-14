"""Service AuthN for destructive Snuba endpoints.

Near-term: short-lived HS256 JWT, audience-bound to snuba deletes.
Long-term: replace ``authenticate_service_request`` with mesh / mTLS
workload identity. AuthZ (predicate ids) consumes ``ServiceIdentity``
and must not be rewritten when the verifier changes.

Never take the principal or tenant ids from the request body.
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
from jwt import InvalidTokenError

from snuba import environment, settings
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger("snuba.service_auth")
metrics = MetricsWrapper(environment.metrics, "snuba.delete.auth")

DELETE_JWT_AUDIENCE = "snuba-deletes"
DELETE_JWT_ALGORITHM = "HS256"
SENTRY_DELETE_PRINCIPAL = "sentry-delete"


class ServiceAuthError(Exception):
    """Unauthenticated or misconfigured delete AuthN. Maps to HTTP 401."""

    status_code = 401


class ServiceAuthzError(Exception):
    """Authenticated but not allowed to delete. Maps to HTTP 403."""

    status_code = 403


@dataclass(frozen=True)
class ServiceIdentity:
    """Verified caller. Tenant id sets are empty until a delegated claim is present."""

    principal: str
    source: str
    authorized_project_ids: frozenset[int]
    authorized_organization_ids: frozenset[int]


_active_identity: ContextVar[ServiceIdentity | None] = ContextVar(
    "snuba_delete_service_identity", default=None
)


def _auth_secret() -> str:
    return str(getattr(settings, "DELETE_SERVICE_AUTH_SECRET", "") or "")


def _allowed_principals() -> frozenset[str]:
    raw = getattr(settings, "DELETE_SERVICE_AUTH_ALLOWED_PRINCIPALS", (SENTRY_DELETE_PRINCIPAL,))
    return frozenset(raw)


def _max_ttl_seconds() -> int:
    return int(getattr(settings, "DELETE_SERVICE_AUTH_MAX_TTL_SECONDS", 120))


def service_auth_is_enforced() -> bool:
    """Fail closed outside tests when the secret is missing."""
    if _auth_secret():
        return True
    return not bool(getattr(settings, "TESTING", False))


def authenticate_service_request(headers: Mapping[str, str]) -> ServiceIdentity:
    """Verify service identity from request headers. Never reads the body."""
    endpoint = headers.get("x-snuba-delete-endpoint", "unknown")
    secret = _auth_secret()
    if not secret:
        metrics.increment("failure", tags={"reason": "missing_secret", "endpoint": endpoint})
        logger.warning("delete_auth_misconfigured")
        raise ServiceAuthError("delete service auth is not configured")

    authorization = _header(headers, "Authorization")
    if not authorization:
        metrics.increment("failure", tags={"reason": "missing", "endpoint": endpoint})
        raise ServiceAuthError("missing authorization")

    scheme, _, credential = authorization.partition(" ")
    if scheme.lower() != "bearer" or not credential:
        metrics.increment("failure", tags={"reason": "invalid_scheme", "endpoint": endpoint})
        raise ServiceAuthError("invalid authorization")

    try:
        payload = jwt.decode(
            credential,
            secret,
            algorithms=[DELETE_JWT_ALGORITHM],
            audience=DELETE_JWT_AUDIENCE,
            options={"require": ["exp", "iat", "sub", "aud"]},
        )
    except InvalidTokenError:
        metrics.increment("failure", tags={"reason": "invalid_token", "endpoint": endpoint})
        raise ServiceAuthError("invalid authorization") from None

    principal = payload.get("sub")
    if not isinstance(principal, str) or principal not in _allowed_principals():
        metrics.increment("failure", tags={"reason": "invalid_principal", "endpoint": endpoint})
        raise ServiceAuthError("invalid authorization")

    iat = payload.get("iat")
    exp = payload.get("exp")
    if not isinstance(iat, int) or not isinstance(exp, int) or exp - iat > _max_ttl_seconds():
        metrics.increment("failure", tags={"reason": "ttl", "endpoint": endpoint})
        raise ServiceAuthError("invalid authorization")

    identity = ServiceIdentity(
        principal=principal,
        source="jwt",
        authorized_project_ids=_int_set(payload.get("project_ids")),
        authorized_organization_ids=_int_set(payload.get("organization_ids")),
    )
    metrics.increment("success", tags={"principal": identity.principal, "endpoint": endpoint})
    return identity


def require_delete_identity() -> ServiceIdentity:
    identity = _active_identity.get()
    if identity is None:
        raise ServiceAuthError("missing service identity")
    return identity


def bind_service_identity(identity: ServiceIdentity) -> Token[ServiceIdentity | None]:
    return _active_identity.set(identity)


def reset_service_identity(token: Token[ServiceIdentity | None]) -> None:
    _active_identity.reset(token)


@contextmanager
def using_service_identity(identity: ServiceIdentity) -> Iterator[ServiceIdentity]:
    token = bind_service_identity(identity)
    try:
        yield identity
    finally:
        reset_service_identity(token)


def mint_delete_service_token(
    *,
    principal: str = SENTRY_DELETE_PRINCIPAL,
    project_ids: Collection[int] = (),
    organization_ids: Collection[int] = (),
    ttl_seconds: int | None = None,
    secret: str | None = None,
) -> str:
    """Test/caller helper. Production callers live in sentry."""
    key = secret if secret is not None else _auth_secret()
    if not key:
        raise ServiceAuthError("delete service auth is not configured")
    now = datetime.now(tz=UTC)
    lifetime = ttl_seconds if ttl_seconds is not None else min(60, _max_ttl_seconds())
    payload = {
        "iss": "sentry",
        "aud": DELETE_JWT_AUDIENCE,
        "sub": principal,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=lifetime)).timestamp()),
        "project_ids": [int(pid) for pid in project_ids],
        "organization_ids": [int(oid) for oid in organization_ids],
    }
    return jwt.encode(payload, key, algorithm=DELETE_JWT_ALGORITHM)


def _header(headers: Mapping[str, str], name: str) -> str:
    target = name.lower()
    for key, value in headers.items():
        if key.lower() == target:
            return value
    return ""


def _int_set(raw: Any) -> frozenset[int]:
    if not isinstance(raw, list):
        return frozenset()
    values: set[int] = set()
    for item in raw:
        if isinstance(item, bool) or not isinstance(item, int):
            raise ServiceAuthError("invalid authorization")
        values.add(item)
    return frozenset(values)
