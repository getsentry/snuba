from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
import pytest

from snuba.web.service_auth import (
    DELETE_JWT_ALGORITHM,
    DELETE_JWT_AUDIENCE,
    SENTRY_DELETE_PRINCIPAL,
    ServiceAuthError,
    authenticate_service_request,
    mint_delete_service_token,
)
from tests.base import BaseApiTest


def test_authenticate_missing_header() -> None:
    with pytest.raises(ServiceAuthError, match="missing authorization"):
        authenticate_service_request({})


def test_authenticate_invalid_scheme() -> None:
    with pytest.raises(ServiceAuthError, match="invalid authorization"):
        authenticate_service_request({"Authorization": "Basic abc"})


def test_authenticate_invalid_token() -> None:
    with pytest.raises(ServiceAuthError, match="invalid authorization"):
        authenticate_service_request({"Authorization": "Bearer not-a-jwt"})


def test_authenticate_wrong_audience() -> None:
    token = jwt.encode(
        {
            "sub": SENTRY_DELETE_PRINCIPAL,
            "aud": "someone-else",
            "iat": int(datetime.now(tz=UTC).timestamp()),
            "exp": int((datetime.now(tz=UTC) + timedelta(seconds=30)).timestamp()),
        },
        "snuba-test-delete-service-auth-secret",
        algorithm=DELETE_JWT_ALGORITHM,
    )
    with pytest.raises(ServiceAuthError, match="invalid authorization"):
        authenticate_service_request({"Authorization": f"Bearer {token}"})


def test_authenticate_wrong_principal() -> None:
    token = mint_delete_service_token(principal="sentry-query")
    with pytest.raises(ServiceAuthError, match="invalid authorization"):
        authenticate_service_request({"Authorization": f"Bearer {token}"})


def test_authenticate_valid_token() -> None:
    token = mint_delete_service_token(project_ids=[1, 2], organization_ids=[9])
    identity = authenticate_service_request({"Authorization": f"Bearer {token}"})
    assert identity.principal == SENTRY_DELETE_PRINCIPAL
    assert identity.source == "jwt"
    assert identity.authorized_project_ids == frozenset({1, 2})
    assert identity.authorized_organization_ids == frozenset({9})


def test_authenticate_ignores_body_like_headers() -> None:
    token = mint_delete_service_token(principal=SENTRY_DELETE_PRINCIPAL)
    identity = authenticate_service_request(
        {
            "Authorization": f"Bearer {token}",
            "X-Principal": "attacker",
            "tenant_ids": "nope",
        }
    )
    assert identity.principal == SENTRY_DELETE_PRINCIPAL
    assert DELETE_JWT_AUDIENCE == "snuba-deletes"


class TestDeleteServiceAuthHTTP(BaseApiTest):
    def test_rpc_delete_rejects_missing_auth(self) -> None:
        response = self.app.post(
            "/rpc/EndpointDeleteTraceItems/v1",
            data=b"",
            headers={"referer": "test"},
        )
        assert response.status_code == 401

    def test_rpc_delete_rejects_invalid_auth(self) -> None:
        response = self.app.post(
            "/rpc/EndpointDeleteTraceItems/v1",
            data=b"",
            headers={"referer": "test", "Authorization": "Bearer not-a-jwt"},
        )
        assert response.status_code == 401

    def test_rpc_delete_accepts_valid_auth_before_parse(self) -> None:
        token = mint_delete_service_token()
        response = self.app.post(
            "/rpc/EndpointDeleteTraceItems/v1",
            data=b"not-valid-protobuf",
            headers={"referer": "test", "Authorization": f"Bearer {token}"},
        )
        # Auth passed; protobuf decode fails closed as 400.
        assert response.status_code == 400
