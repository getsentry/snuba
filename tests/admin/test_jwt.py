from __future__ import annotations

import base64
import json
import time
from unittest.mock import patch

import jwt
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

from snuba.admin.jwt import validate_assertion

AUDIENCE = "test-audience"
KID = "test-kid"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _make_es256_jwt(
    private_key: ec.EllipticCurvePrivateKey,
    payload: dict[str, object],
    kid: str = KID,
) -> str:
    header = {"alg": "ES256", "typ": "JWT", "kid": kid}
    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode())
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{header_b64}.{payload_b64}".encode()

    der_sig = private_key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(der_sig)
    raw_sig = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    sig_b64 = _b64url(raw_sig)

    return f"{header_b64}.{payload_b64}.{sig_b64}"


@pytest.fixture
def keypair() -> tuple[ec.EllipticCurvePrivateKey, str]:
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_pem = (
        private_key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private_key, public_pem


def test_validate_assertion_decodes_es256_token(
    keypair: tuple[ec.EllipticCurvePrivateKey, str],
) -> None:
    private_key, public_pem = keypair
    now = int(time.time())
    token = _make_es256_jwt(
        private_key,
        {
            "email": "alice@example.com",
            "sub": "user-42",
            "aud": AUDIENCE,
            "iat": now,
            "exp": now + 3600,
        },
    )

    with (
        patch("snuba.admin.jwt._certs", return_value={KID: public_pem}),
        patch("snuba.admin.jwt._audience", return_value=AUDIENCE),
    ):
        user = validate_assertion(token)

    assert user.email == "alice@example.com"
    assert user.id == "user-42"


def test_validate_assertion_rejects_wrong_audience(
    keypair: tuple[ec.EllipticCurvePrivateKey, str],
) -> None:
    private_key, public_pem = keypair
    now = int(time.time())
    token = _make_es256_jwt(
        private_key,
        {
            "email": "alice@example.com",
            "sub": "user-42",
            "aud": "some-other-audience",
            "iat": now,
            "exp": now + 3600,
        },
    )

    with (
        patch("snuba.admin.jwt._certs", return_value={KID: public_pem}),
        patch("snuba.admin.jwt._audience", return_value=AUDIENCE),
        pytest.raises(jwt.InvalidAudienceError),
    ):
        validate_assertion(token)


def test_validate_assertion_rejects_bad_signature(
    keypair: tuple[ec.EllipticCurvePrivateKey, str],
) -> None:
    private_key, _ = keypair
    now = int(time.time())
    token = _make_es256_jwt(
        private_key,
        {
            "email": "alice@example.com",
            "sub": "user-42",
            "aud": AUDIENCE,
            "iat": now,
            "exp": now + 3600,
        },
    )

    other_key = ec.generate_private_key(ec.SECP256R1())
    other_pem = (
        other_key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )

    with (
        patch("snuba.admin.jwt._certs", return_value={KID: other_pem}),
        patch("snuba.admin.jwt._audience", return_value=AUDIENCE),
        pytest.raises(jwt.InvalidSignatureError),
    ):
        validate_assertion(token)
