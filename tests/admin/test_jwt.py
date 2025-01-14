from datetime import datetime, timedelta, timezone
from typing import Generator
from unittest.mock import MagicMock, patch

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from snuba.admin.jwt import validate_assertion
from snuba.admin.user import AdminUser


@pytest.fixture
def key_pair() -> tuple[bytes, bytes]:
    """Generate a fresh ES256 key pair for each test run"""
    private_key = ec.generate_private_key(ec.SECP256K1())
    public_key = private_key.public_key()

    # Convert to PEM format
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    return private_pem, public_pem


TEST_AUDIENCE = "test-audience"
TEST_EMAIL = "test@example.com"
TEST_SUB = "12345"


@pytest.fixture
def mock_settings() -> Generator[MagicMock, None, None]:
    with patch("snuba.admin.jwt.settings") as mock_settings:
        mock_settings.ADMIN_AUTH_JWT_AUDIENCE = TEST_AUDIENCE
        yield mock_settings


@pytest.fixture
def mock_certs(key_pair: tuple[bytes, bytes]) -> Generator[MagicMock, None, None]:
    _, public_key = key_pair
    with patch("snuba.admin.jwt._certs") as mock_certs:
        mock_certs.return_value = public_key.decode("utf-8")
        yield mock_certs


def create_test_token(
    private_key: bytes,
    *,
    email: str = TEST_EMAIL,
    sub: str = TEST_SUB,
    audience: str = TEST_AUDIENCE,
    exp_delta: timedelta = timedelta(hours=1),
) -> str:
    """Helper function to create a test JWT token"""
    now = datetime.now(timezone.utc)
    payload = {
        "email": email,
        "sub": sub,
        "aud": audience,
        "exp": now + exp_delta,
        "iat": now,
    }
    return jwt.encode(payload, private_key.decode("utf-8"), algorithm="ES256")


def test_validate_assertion_success(
    mock_settings: MagicMock, mock_certs: MagicMock, key_pair: tuple[bytes, bytes]
) -> None:
    """Test that a valid JWT token is properly validated and returns an AdminUser"""
    private_key, _ = key_pair
    token = create_test_token(private_key)
    user = validate_assertion(token)
    assert isinstance(user, AdminUser)
    assert user.email == TEST_EMAIL
    assert user.id == TEST_SUB


def test_validate_assertion_invalid_signature(
    mock_settings: MagicMock, mock_certs: MagicMock
) -> None:
    """Test that an invalid signature raises an exception"""
    different_private_key = ec.generate_private_key(ec.SECP256K1()).private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    token = create_test_token(different_private_key)
    with pytest.raises(jwt.InvalidSignatureError):
        validate_assertion(token)


def test_validate_assertion_expired(
    mock_settings: MagicMock, mock_certs: MagicMock, key_pair: tuple[bytes, bytes]
) -> None:
    """Test that an expired token raises an exception"""
    private_key, _ = key_pair
    token = create_test_token(private_key, exp_delta=timedelta(hours=-1))
    with pytest.raises(jwt.ExpiredSignatureError):
        validate_assertion(token)


def test_validate_assertion_invalid_audience(
    mock_settings: MagicMock, mock_certs: MagicMock, key_pair: tuple[bytes, bytes]
) -> None:
    """Test that a token with wrong audience raises an exception"""
    private_key, _ = key_pair
    token = create_test_token(private_key, audience="wrong-audience")
    with pytest.raises(jwt.InvalidAudienceError):
        validate_assertion(token)


def test_validate_assertion_missing_email(
    mock_settings: MagicMock, mock_certs: MagicMock, key_pair: tuple[bytes, bytes]
) -> None:
    """Test that a token without email claim raises an exception"""
    private_key, _ = key_pair
    now = datetime.now(timezone.utc)
    payload = {
        "sub": TEST_SUB,
        "aud": TEST_AUDIENCE,
        "exp": now + timedelta(hours=1),
        "iat": now,
    }
    token = jwt.encode(payload, private_key.decode("utf-8"), algorithm="ES256")
    with pytest.raises(KeyError):
        validate_assertion(token)


def test_validate_assertion_missing_sub(
    mock_settings: MagicMock, mock_certs: MagicMock, key_pair: tuple[bytes, bytes]
) -> None:
    """Test that a token without sub claim raises an exception"""
    private_key, _ = key_pair
    now = datetime.now(timezone.utc)
    payload = {
        "email": TEST_EMAIL,
        "aud": TEST_AUDIENCE,
        "exp": now + timedelta(hours=1),
        "iat": now,
    }
    token = jwt.encode(payload, private_key.decode("utf-8"), algorithm="ES256")
    with pytest.raises(KeyError):
        validate_assertion(token)
